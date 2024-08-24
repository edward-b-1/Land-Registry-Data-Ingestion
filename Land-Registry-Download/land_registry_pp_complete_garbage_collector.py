
import signal
import jsons
from datetime import datetime
from datetime import timezone
from datetime import timedelta

from confluent_kafka import Consumer
from confluent_kafka import Producer

from sqlalchemy import Engine
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import boto3
import botocore

from lib_land_registry_data.lib_kafka import create_consumer
from lib_land_registry_data.lib_kafka import create_producer

from lib_land_registry_data.lib_constants.process_name import PROCESS_NAME_PP_COMPLETE_GARBAGE_COLLECTOR

from lib_land_registry_data.lib_topic_name import TOPIC_NAME_PP_COMPLETE_DATA_DECISION_NOTIFICATION
from lib_land_registry_data.lib_topic_name import TOPIC_NAME_PP_COMPLETE_GC_NOTIFICATION

from lib_land_registry_data.lib_constants.notification_type import NOTIFICATION_TYPE_PP_COMPLETE_DATA_DECISION_COMPLETE
from lib_land_registry_data.lib_constants.notification_type import NOTIFICATION_TYPE_PP_COMPLETE_GC_COMPLETE

from lib_land_registry_data.lib_dto import PPCompleteDataDecisionNotificationDTO
from lib_land_registry_data.lib_dto import PPCompleteGCNotificationDTO

from lib_land_registry_data.lib_db import PPCompleteDownloadFileLog

from lib_land_registry_data.lib_env import EnvironmentVariables

from lib_land_registry_data.logging import set_logger_process_name
from lib_land_registry_data.logging import get_logger
from lib_land_registry_data.logging import create_stdout_log_handler
from lib_land_registry_data.logging import create_file_log_handler


set_logger_process_name(
    process_name=PROCESS_NAME_PP_COMPLETE_GARBAGE_COLLECTOR,
)

logger = get_logger()
stdout_log_handler = create_stdout_log_handler()
file_log_handler = create_file_log_handler(
    logger_process_name=PROCESS_NAME_PP_COMPLETE_GARBAGE_COLLECTOR,
    logger_file_datetime=datetime.now(timezone.utc).date(),
)
logger.addHandler(stdout_log_handler)
logger.addHandler(file_log_handler)


def main():
    logger.info(f'{PROCESS_NAME_PP_COMPLETE_GARBAGE_COLLECTOR} start')

    environment_variables = EnvironmentVariables()

    kafka_bootstrap_servers = environment_variables.get_kafka_bootstrap_servers()
    logger.info(f'create kafka consumer producer: bootstrap_servers={kafka_bootstrap_servers}')

    logger.info(f'create consumer')
    consumer = create_consumer(
        bootstrap_servers=kafka_bootstrap_servers,
        client_id=PROCESS_NAME_PP_COMPLETE_GARBAGE_COLLECTOR,
        group_id=PROCESS_NAME_PP_COMPLETE_GARBAGE_COLLECTOR,
    )

    logger.info(f'create producer')
    producer = create_producer(
        bootstrap_servers=kafka_bootstrap_servers,
        client_id=PROCESS_NAME_PP_COMPLETE_GARBAGE_COLLECTOR,
    )

    aws_access_key_id = environment_variables.get_aws_access_key_id()
    aws_secret_access_key = environment_variables.get_aws_secret_access_key()
    minio_url = environment_variables.get_minio_url()

    # TODO: move to library code
    logger.info(f'create boto3 session: minio_url={minio_url}')
    boto3_session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    postgres_connection_string = environment_variables.get_postgres_connection_string()
    logger.info(f'create database engine: postgres_host={environment_variables.get_postgres_host()}')
    postgres_engine = create_engine(postgres_connection_string)

    logger.info(f'run event loop')
    kafka_event_loop(
        consumer=consumer,
        producer=producer,
        boto3_session=boto3_session,
        minio_url=minio_url,
        postgres_engine=postgres_engine,
    )


def kafka_event_loop(
    consumer: Consumer,
    producer: Producer,
    boto3_session,
    minio_url: str,
    postgres_engine: Engine,
) -> None:

    logger.info(f'consumer subscribing to topic {NOTIFICATION_TYPE_PP_COMPLETE_DATA_DECISION_COMPLETE}')
    consumer.subscribe([NOTIFICATION_TYPE_PP_COMPLETE_DATA_DECISION_COMPLETE])
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
            log_message = f'kafka message error: {message.value().decode()}'
            logger.error(log_message)
            raise RuntimeError(f'{message.value().decode()}')
        else:
            dto = jsons.loads(
                message.value().decode(),
                PPCompleteDataDecisionNotificationDTO,
            )

            try:
                notification_type = dto.notification_type
                logger.debug(f'notification type: {notification_type}')

                if notification_type == NOTIFICATION_TYPE_PP_COMPLETE_DATA_DECISION_COMPLETE:
                    logger.info(f'run garbage collection process')

                    # new notification documents just trigger the garbage collection
                    # process for all old files
                    garbage_collect(
                        producer=producer,
                        postgres_engine=postgres_engine,
                        boto3_session=boto3_session,
                        minio_url=minio_url,
                    )

                else:
                    raise RuntimeError(f'unknown notification type: {notification_type}')

            except Exception as exception:
                log_message = f'notification error: {exception}'
                logger.error(log_message)

    consumer.unsubscribe()
    consumer.close()


def garbage_collect(
    producer: Producer,
    postgres_engine: Engine,
    boto3_session,
    minio_url: str,
) -> None:

    with Session(postgres_engine) as session:

        rows = (
            session
            .query(PPCompleteDownloadFileLog)
            .filter(PPCompleteDownloadFileLog.gc_action_taken.is_(None))
            .all()
        )
        logger.info(f'querying database table PPCompleteDownloadFileLog for rows with process_decision=\'garbage_collect\'')
        logger.info(f'{len(rows)} match database query')

        for row in rows:
            data_decision = row.data_decision

            if data_decision == 'archive':
                logger.info(f'row with data_decision={data_decision}, ignore')
                row.gc_action_taken = 'ignore'
                row.gc_datetime = datetime.now(timezone.utc)
                session.commit()

            elif data_decision == 'garbage_collect':
                logger.info(f'row with data_decision={data_decision}, run garbage collection')

                bucket = row.s3_tmp_bucket
                object_key = row.s3_tmp_object_key
                logger.info(f'delete file from s3 in bucket {bucket} with object key {object_key}')

                boto3_client = (
                    boto3_session.client(
                        's3',
                        endpoint_url=minio_url,
                        config=botocore.Config(signature_version='s3v4'),
                    )
                )
                boto3_client.delete_object(Bucket=bucket, Key=object_key)

                row.gc_action_taken = 'garbage_collect'
                row.gc_datetime = datetime.now(timezone.utc)
                session.commit()

            else:
                raise RuntimeError(f'invalid data_decision {data_decision}')

            pp_complete_file_log_id = row.pp_complete_file_log_id

            notify(
                producer=producer,
                pp_complete_file_log_id=pp_complete_file_log_id,
            )


def notify(
    producer: Producer,
    pp_complete_file_log_id: int,
) -> None:

    dto = PPCompleteGCNotificationDTO(
        notification_source=PROCESS_NAME_PP_COMPLETE_GARBAGE_COLLECTOR,
        notification_type=NOTIFICATION_TYPE_PP_COMPLETE_GC_COMPLETE,
        notification_timestamp=datetime.now(timezone.utc),
        pp_complete_file_log_id=pp_complete_file_log_id,
    )

    dto_json_str = jsons.dumps(dto, strip_privates=True)

    producer.produce(
        topic=TOPIC_NAME_PP_COMPLETE_GC_NOTIFICATION,
        key=f'no_key',
        value=dto_json_str,
    )
    producer.flush()


exit_flag = False

def ctrl_c_signal_handler(signal, frame):
    logger.info(f'CTRL^C wait for exit...')
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


