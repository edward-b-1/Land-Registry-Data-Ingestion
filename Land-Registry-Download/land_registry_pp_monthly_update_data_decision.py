
import signal
import threading
import jsons
from datetime import datetime
from datetime import timezone

from confluent_kafka import Consumer
from confluent_kafka import Producer

from sqlalchemy import Engine
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from lib_land_registry_data.lib_constants.process_name import PROCESS_NAME_LAND_REGISTRY_DATA_PP_MONTHLY_UPDATE_DATA_DECISION
from lib_land_registry_data.lib_constants.process_name import PROCESS_NAME_LAND_REGISTRY_DATA_PP_MONTHLY_UPDATE_DOWNLOADER

from lib_land_registry_data.lib_topic_name import TOPIC_NAME_LAND_REGISTRY_DATA_PP_MONTHLY_UPDATE_DOWNLOAD_NOTIFICATION
from lib_land_registry_data.lib_topic_name import TOPIC_NAME_LAND_REGISTRY_DATA_PP_MONTHLY_UPDATE_DATA_DECISION_NOTIFICATION

from lib_land_registry_data.lib_constants.notification_type import NOTIFICATION_TYPE_PP_MONTHLY_UPDATE_DOWNLOAD_COMPLETE
from lib_land_registry_data.lib_constants.notification_type import NOTIFICATION_TYPE_PP_MONTHLY_UPDATE_DATA_DECISION_COMPLETE

from lib_land_registry_data.lib_kafka import create_consumer
from lib_land_registry_data.lib_kafka import create_producer

from lib_land_registry_data.lib_dto import PPMonthlyUpdateDownloadCompleteNotificationDTO
from lib_land_registry_data.lib_dto import PPMonthlyUpdateDataDecisionNotificationDTO

from lib_land_registry_data.lib_db import PPMonthlyUpdateDownloadFileLog

from lib_land_registry_data.lib_env import EnvironmentVariables

from lib_land_registry_data.logging import set_logger_process_name
from lib_land_registry_data.logging import get_logger
from lib_land_registry_data.logging import create_stdout_log_handler
from lib_land_registry_data.logging import create_file_log_handler


set_logger_process_name(
    process_name=PROCESS_NAME_LAND_REGISTRY_DATA_PP_MONTHLY_UPDATE_DATA_DECISION,
)

logger = get_logger()
stdout_log_handler = create_stdout_log_handler()
file_log_handler = create_file_log_handler(
    logger_process_name=PROCESS_NAME_LAND_REGISTRY_DATA_PP_MONTHLY_UPDATE_DATA_DECISION,
    logger_file_datetime=datetime.now(timezone.utc).date(),
)
logger.addHandler(stdout_log_handler)
logger.addHandler(file_log_handler)


def main():
    logger.info(f'{PROCESS_NAME_LAND_REGISTRY_DATA_PP_MONTHLY_UPDATE_DATA_DECISION} start')

    environment_variables = EnvironmentVariables()
    kafka_bootstrap_servers = environment_variables.get_kafka_bootstrap_servers()
    postgres_connection_string = environment_variables.get_postgres_connection_string()

    logger.info(f'create consumer')
    consumer = create_consumer(
        bootstrap_servers=kafka_bootstrap_servers,
        client_id=PROCESS_NAME_LAND_REGISTRY_DATA_PP_MONTHLY_UPDATE_DATA_DECISION,
        group_id=PROCESS_NAME_LAND_REGISTRY_DATA_PP_MONTHLY_UPDATE_DATA_DECISION,
    )

    logger.info(f'create producer')
    producer = create_producer(
        bootstrap_servers=kafka_bootstrap_servers,
        client_id=PROCESS_NAME_LAND_REGISTRY_DATA_PP_MONTHLY_UPDATE_DATA_DECISION,
    )

    logger.info(f'create engine')
    postgres_engine = create_engine(postgres_connection_string)

    logger.info(f'run controller process')
    kafka_event_loop(
        consumer=consumer,
        producer=producer,
        postgres_engine=postgres_engine,
    )


def kafka_event_loop(
    consumer: Consumer,
    producer: Producer,
    postgres_engine: Engine,
) -> None:

    logger.info(f'consumer subscribing to topic {TOPIC_NAME_LAND_REGISTRY_DATA_PP_MONTHLY_UPDATE_DOWNLOAD_NOTIFICATION}')
    consumer.subscribe([TOPIC_NAME_LAND_REGISTRY_DATA_PP_MONTHLY_UPDATE_DOWNLOAD_NOTIFICATION])
    consumer_poll_timeout = 10.0
    logger.info(f'consumer poll timeout: {consumer_poll_timeout}')

    global exit_flag

    while not exit_flag:

        # provide a short period flush to allow the producer to catch up
        # with dispatched events
        producer.flush(3.0)
        producer.poll(1.0)
        message = consumer.poll(consumer_poll_timeout)

        if message is None:
            continue

        if message.error():
            logger.error(f'kafka message error: {message.value().decode()}')
            raise RuntimeError(f'{message.value().decode()}')
        else:
            logger.debug(f'message received')
            dto = jsons.loads(
                message.value().decode(),
                PPMonthlyUpdateDownloadCompleteNotificationDTO,
            )

            try:
                notification_type = dto.notification_type
                logger.debug(f'message type {notification_type}')

                if notification_type == NOTIFICATION_TYPE_PP_MONTHLY_UPDATE_DOWNLOAD_COMPLETE:

                    pp_monthly_update_file_log_id = dto.pp_monthly_update_file_log_id

                    with Session(postgres_engine) as session:
                        row = (
                            session
                            .query(PPMonthlyUpdateDownloadFileLog)
                            .filter_by(pp_monthly_update_file_log_id=pp_monthly_update_file_log_id)
                            .one()
                        )

                        rows_with_data_decision_archive = (
                            session
                            .query(PPMonthlyUpdateDownloadFileLog)
                            .filter_by(data_decision='archive')
                            .order_by(PPMonthlyUpdateDownloadFileLog.data_decsion_datetime)
                            .all()
                        )
                        logger.debug(f'number of database rows with data_decision=\'archive\': {len(rows_with_data_decision_archive)}')

                        if len(rows_with_data_decision_archive) < 1:
                            logger.debug(f'no previous rows with data_decision=\'archive\', current file will be archived')
                            row.data_decision = 'archive'
                            row.data_decision_datetime = datetime.now(timezone.utc)
                            session.commit()
                        else:
                            last_row_with_data_decision_archive = (
                                rows_with_data_decision_archive[-1]
                            )

                            if last_row_with_data_decision_archive.sha256sum != row.sha256sum:
                                logger.debug(f'previous row with data_decision=\'archive\' has different hash, current file will be archived')
                                logger.debug(f'hash: {row.sha256sum}, previous hash: {last_row_with_data_decision_archive.sha256sum}')
                                row.data_decision = 'archive'
                                row.data_decision_datetime = datetime.now(timezone.utc)
                                session.commit()
                            else:
                                logger.debug(f'previous row with data_decision=\'archive\' has same hash, current file will be deleted')
                                logger.debug(f'hash: {row.sha256sum}')
                                row.data_decision = 'garbage_collect'
                                row.data_decision_datetime = datetime.now(timezone.utc)
                                session.commit()

                    notify(
                        producer=producer,
                        pp_monthly_update_file_log_id=pp_monthly_update_file_log_id,
                    )

                else:
                    raise RuntimeError(f'unknown notification type: {notification_type}')

            except Exception as exception:
                logger.error(f'{exception}')

    consumer.unsubscribe()
    consumer.close()


def notify(
    producer: Producer,
    pp_monthly_update_file_log_id: int,
) -> None:

    dto = PPMonthlyUpdateDataDecisionNotificationDTO(
        notification_source=PROCESS_NAME_LAND_REGISTRY_DATA_PP_MONTHLY_UPDATE_DATA_DECISION,
        notification_type=NOTIFICATION_TYPE_PP_MONTHLY_UPDATE_DATA_DECISION_COMPLETE,
        notification_timestamp=datetime.now(timezone.utc),
        pp_monthly_update_file_log_id=pp_monthly_update_file_log_id,
    )

    dto_json_str = jsons.dumps(dto, strip_privates=True)

    producer.produce(
        topic=TOPIC_NAME_LAND_REGISTRY_DATA_PP_MONTHLY_UPDATE_DATA_DECISION_NOTIFICATION,
        key=f'no_key',
        value=dto_json_str,
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


