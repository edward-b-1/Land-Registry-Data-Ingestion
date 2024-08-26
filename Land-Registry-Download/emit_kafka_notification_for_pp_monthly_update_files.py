
'''
This process sends a notification to the kafka topic to cause the database
update process to run.

This process takes pp-monthly-update.txt files and updates the database using
the data from each of these files.

A notification should be sent, which causes the database update processes to run,
whenever a new monthly update file is added to the archive. However, this will
not happen when the system is first initialized using the historical data files.
So this process is used to manually emit a notification.
'''


import jsons

from datetime import datetime
from datetime import timezone

from confluent_kafka import Consumer
from confluent_kafka import Producer

from sqlalchemy import Engine
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from lib_land_registry_data.lib_kafka import create_producer

from lib_land_registry_data.lib_topic_name import TOPIC_NAME_PP_MONTHLY_UPDATE_ARCHIVE_NOTIFICATION

from lib_land_registry_data.lib_constants.notification_type import NOTIFICATION_TYPE_PP_MONTHLY_UPDATE_ARCHIVE_COMPLETE

from lib_land_registry_data.lib_dto import PPMonthlyUpdateArchiveNotificationDTO

from lib_land_registry_data.lib_env import EnvironmentVariables

from lib_land_registry_data.logging import set_logger_process_name
from lib_land_registry_data.logging import get_logger
from lib_land_registry_data.logging import create_stdout_log_handler
from lib_land_registry_data.logging import create_file_log_handler

from lib_land_registry_data.lib_db import PPMonthlyUpdateArchiveFileLog


PROCESS_NAME = 'pp_monthly_update_manual_notify'

set_logger_process_name(
    process_name=PROCESS_NAME,
)

logger = get_logger()
stdout_log_handler = create_stdout_log_handler()
file_log_handler = create_file_log_handler(
    logger_process_name=PROCESS_NAME,
    logger_file_datetime=datetime.now(timezone.utc).date(),
)
logger.addHandler(stdout_log_handler)
logger.addHandler(file_log_handler)


def main():
    logger.info(f'{PROCESS_NAME} start')

    environment_variables = EnvironmentVariables()

    kafka_bootstrap_servers = environment_variables.get_kafka_bootstrap_servers()
    logger.info(f'create kafka consumer producer: bootstrap_servers={kafka_bootstrap_servers}')

    logger.info(f'create producer')
    producer = create_producer(
        bootstrap_servers=kafka_bootstrap_servers,
        client_id=PROCESS_NAME,
    )

    postgres_connection_string = environment_variables.get_postgres_connection_string()
    logger.info(f'create database engine: postgres_host={environment_variables.get_postgres_host()}')
    postgres_engine = create_engine(postgres_connection_string)

    with Session(postgres_engine) as session:

        rows = (
            session
            .query(PPMonthlyUpdateArchiveFileLog)
            .order_by(PPMonthlyUpdateArchiveFileLog.pp_monthly_update_archive_file_log_id)
            .all()
        )

        for row in rows:
            pp_monthly_update_archive_file_log_id = row.pp_monthly_update_archive_file_log_id

            logger.info(f'processing row with {pp_monthly_update_archive_file_log_id=}')

            database_update_start_timestamp = row.database_update_start_timestamp

            if database_update_start_timestamp is None:
                notify(
                    producer=producer,
                    pp_monthly_update_archive_file_log_id=pp_monthly_update_archive_file_log_id,
                )
            else:
                logger.info(f'ignoring row with non null {database_update_start_timestamp=}')


def notify(
    producer: Producer,
    pp_monthly_update_archive_file_log_id: int,
) -> None:
    logger.debug(f'sending notification')

    dto = PPMonthlyUpdateArchiveNotificationDTO(
        notification_source=PROCESS_NAME,
        notification_type=NOTIFICATION_TYPE_PP_MONTHLY_UPDATE_ARCHIVE_COMPLETE,
        notification_timestamp=datetime.now(timezone.utc),
        pp_monthly_update_archive_file_log_id=pp_monthly_update_archive_file_log_id,
    )

    dto_json_str = jsons.dumps(dto, strip_privates=True)

    producer.produce(
        topic=TOPIC_NAME_PP_MONTHLY_UPDATE_ARCHIVE_NOTIFICATION,
        key=f'no_key',
        value=dto_json_str,
    )
    producer.flush()
    logger.debug(f'notification sent')



if __name__ == '__main__':
    main()

