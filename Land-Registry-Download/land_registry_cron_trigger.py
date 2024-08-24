

from datetime import datetime
from datetime import date
from datetime import timedelta
from datetime import timezone

import jsons
import signal

from confluent_kafka import Producer

from sqlalchemy import create_engine
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from lib_land_registry_data.lib_constants.process_name import PROCESS_NAME_CRON_TRIGGER

from lib_land_registry_data.lib_topic_name import TOPIC_NAME_CRON_TRIGGER_NOTIFICATION

from lib_land_registry_data.lib_constants.notification_type import NOTIFICATION_TYPE_CRON_TRIGGER

from lib_land_registry_data.lib_cron import cron_get_next_schedule
from lib_land_registry_data.lib_cron import cron_get_sleep_time
from lib_land_registry_data.lib_cron import cron_get_sleep_time_timeout
from lib_land_registry_data.lib_cron import cron_sleep

from lib_land_registry_data.lib_kafka import create_producer

from lib_land_registry_data.lib_dto import CronTriggerNotificationDTO

from lib_land_registry_data.lib_db import PPCompleteDownloadFileLog
from lib_land_registry_data.lib_db import PPMonthlyUpdateDownloadFileLog

from lib_land_registry_data.lib_env import EnvironmentVariables

from lib_land_registry_data.logging import set_logger_process_name
from lib_land_registry_data.logging import get_logger
from lib_land_registry_data.logging import create_stdout_log_handler
from lib_land_registry_data.logging import create_file_log_handler

# TODO: update this doc
# Processes:
#
# 1. Controller:
#
#   Scheduler for system. Waits on CRON schedule and sends notification
#   to begin download process.
#
# 2. Downloader:
#
#   Waits for notification from Controller and downloads data from Land Registry
#   website. Sends notification when done to Database Uploader.
#
# 3. Database Uploader:
#
#   Waits for notification from Downloader and uploads data from file on disk
#   to database. Performs a reconcilliation process to only upload new rows of
#   data.
#
# 4. Garbage Collector:
#
#   Waits for notification from Database Uploader and removes old files on a
#   schedule based algorithm. Always keeps files with new data. Removes files
#   with duplicated data. The Database Uploader provides this information via
#   the messages sent to Kafka



set_logger_process_name(
    process_name=PROCESS_NAME_CRON_TRIGGER,
)

logger = get_logger()
stdout_log_handler = create_stdout_log_handler()
file_log_handler = create_file_log_handler(
    logger_process_name=PROCESS_NAME_CRON_TRIGGER,
    logger_file_datetime=datetime.now(timezone.utc).date(),
)
logger.addHandler(stdout_log_handler)
logger.addHandler(file_log_handler)


def main():
    logger.info(f'{PROCESS_NAME_CRON_TRIGGER} start')

    environment_variables = EnvironmentVariables()
    kafka_bootstrap_servers = environment_variables.get_kafka_bootstrap_servers()
    postgres_connection_string = environment_variables.get_postgres_connection_string()

    logger.info(f'create producer')
    producer = create_producer(
        bootstrap_servers=kafka_bootstrap_servers,
        client_id=PROCESS_NAME_CRON_TRIGGER,
    )

    logger.info(f'create engine')
    postgres_engine = create_engine(postgres_connection_string)

    logger.info(f'run controller process')
    run_controller_process(producer, postgres_engine)


def run_controller_process(
    producer: Producer,
    engine: Engine,
) -> None:
    global exit_flag

    while not exit_flag:
        logger.info(f'waiting for CRON')

        now = datetime.now(timezone.utc)
        next_schedule = cron_get_next_schedule(now)
        logger.info(f'next_schedule = {next_schedule}')

        # only used to log total sleep duration
        sleep_timedelta = cron_get_sleep_time(now, next_schedule)
        logger.info(f'sleep for {sleep_timedelta}')

        while True:
            if exit_flag:
                logger.info(f'process exit')
                return

            now = datetime.now(timezone.utc)
            if now >= next_schedule:
                break

            sleep_timedelta = cron_get_sleep_time_timeout(now, next_schedule, timeout=timedelta(seconds=10))
            cron_sleep(sleep_timedelta)

        cron_trigger_datetime = datetime.now(timezone.utc)
        cron_target_datetime = next_schedule
        cron_target_date = next_schedule.date()
        update_database(
            engine,
            cron_target_date=cron_target_date,
            cron_target_datetime=cron_target_datetime,
            cron_trigger_datetime=cron_trigger_datetime,
        )
        logger.info(f'database updated')

        notify_trigger(producer)
        logger.info(f'notification sent')

    logger.info(f'process exit')


def update_database(
    engine: Engine,
    cron_target_date: date,
    cron_target_datetime: datetime,
    cron_trigger_datetime: datetime,
) -> None:
    now = datetime.now(timezone.utc)

    with Session(engine) as session:
        row = PPCompleteDownloadFileLog(
            created_datetime=now,
            cron_target_date=cron_target_date,
            cron_target_datetime=cron_target_datetime,
            cron_trigger_datetime=cron_trigger_datetime,
        )
        session.add(row)
        session.commit()

    with Session(engine) as session:
        row = PPMonthlyUpdateDownloadFileLog(
            created_datetime=now,
            cron_target_date=cron_target_date,
            cron_target_datetime=cron_target_datetime,
            cron_trigger_datetime=cron_trigger_datetime,
        )
        session.add(row)
        session.commit()



def notify_trigger(
    producer: Producer,
    pp_complete_file_log_id: int,
    pp_monthly_update_file_log_id: int,
) -> None:
    document = CronTriggerNotificationDTO(
        notification_source=PROCESS_NAME_CRON_TRIGGER,
        notification_type=NOTIFICATION_TYPE_CRON_TRIGGER,
        notification_timestamp=datetime.now(timezone.utc),
        pp_complete_file_log_id=pp_complete_file_log_id,
        pp_monthly_update_file_log_id=pp_monthly_update_file_log_id,
    )

    document_json_str = jsons.dumps(
        document,
        strip_privates=True,
    )

    producer.produce(
        topic=TOPIC_NAME_CRON_TRIGGER_NOTIFICATION,
        key=f'no_key',
        value=document_json_str
    )
    producer.flush()


exit_flag = False

def ctrl_c_signal_handler(signal, frame):
    print(f'wait for exit...')
    global exit_flag
    exit_flag = True

def sigterm_signal_handler(signal, frame):
    logger.info(f'SIGTERM')
    global exit_flag
    exit_flag = True


if __name__ == '__main__':
    signal.signal(signal.SIGINT, ctrl_c_signal_handler)
    signal.signal(signal.SIGTERM, sigterm_signal_handler)
    main()

