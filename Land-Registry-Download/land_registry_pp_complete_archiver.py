
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

from lib_land_registry_data.lib_constants.process_name import PROCESS_NAME_PP_COMPLETE_ARCHIVER

from lib_land_registry_data.lib_constants.topic_name import TOPIC_NAME_PP_COMPLETE_DATA_DECISION_NOTIFICATION
from lib_land_registry_data.lib_constants.topic_name import TOPIC_NAME_PP_COMPLETE_ARCHIVE_NOTIFICATION

from lib_land_registry_data.lib_constants.notification_type import NOTIFICATION_TYPE_PP_COMPLETE_DATA_DECISION_COMPLETE
from lib_land_registry_data.lib_constants.notification_type import NOTIFICATION_TYPE_PP_COMPLETE_ARCHIVE_COMPLETE

from lib_land_registry_data.lib_dto import PPCompleteDataDecisionNotificationDTO
from lib_land_registry_data.lib_dto import PPCompleteArchiveNotificationDTO

from lib_land_registry_data.lib_db import PPCompleteDownloadFileLog
from lib_land_registry_data.lib_db import PPCompleteArchiveFileLog

from lib_land_registry_data.lib_env import EnvironmentVariables

from lib_land_registry_data.logging import set_logger_process_name
from lib_land_registry_data.logging import get_logger
from lib_land_registry_data.logging import create_stdout_log_handler
from lib_land_registry_data.logging import create_file_log_handler


set_logger_process_name(
    process_name=PROCESS_NAME_PP_COMPLETE_ARCHIVER,
)

logger = get_logger()
stdout_log_handler = create_stdout_log_handler()
file_log_handler = create_file_log_handler(
    logger_process_name=PROCESS_NAME_PP_COMPLETE_ARCHIVER,
    logger_file_datetime=datetime.now(timezone.utc).date(),
)
logger.addHandler(stdout_log_handler)
logger.addHandler(file_log_handler)


def main():
    logger.info(f'{PROCESS_NAME_PP_COMPLETE_ARCHIVER} start')

    environment_variables = EnvironmentVariables()

    kafka_bootstrap_servers = environment_variables.get_kafka_bootstrap_servers()
    logger.info(f'create kafka consumer producer: bootstrap_servers={kafka_bootstrap_servers}')

    logger.info(f'create consumer')
    consumer = create_consumer(
        bootstrap_servers=kafka_bootstrap_servers,
        client_id=PROCESS_NAME_PP_COMPLETE_ARCHIVER,
        group_id=PROCESS_NAME_PP_COMPLETE_ARCHIVER,
    )

    logger.info(f'create producer')
    producer = create_producer(
        bootstrap_servers=kafka_bootstrap_servers,
        client_id=PROCESS_NAME_PP_COMPLETE_ARCHIVER,
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

    logger.info(f'consumer subscribing to topic {TOPIC_NAME_PP_COMPLETE_DATA_DECISION_NOTIFICATION}')
    consumer.subscribe([TOPIC_NAME_PP_COMPLETE_DATA_DECISION_NOTIFICATION])
    consumer_poll_timeout = 5.0
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
                    logger.info(f'run archive process')

                    pp_complete_download_file_log_id = dto.pp_complete_download_file_log_id

                    archive(
                        producer=producer,
                        postgres_engine=postgres_engine,
                        boto3_session=boto3_session,
                        minio_url=minio_url,
                        pp_complete_download_file_log_id=pp_complete_download_file_log_id,
                    )
                    consumer.commit()

                else:
                    raise RuntimeError(f'unknown notification type: {notification_type}')

            except Exception as exception:
                log_message = f'notification error: {exception}'
                logger.error(log_message)

    consumer.unsubscribe()
    consumer.close()


def archive(
    producer: Producer,
    postgres_engine: Engine,
    boto3_session,
    minio_url: str,
    pp_complete_download_file_log_id: int,
) -> None:

    with Session(postgres_engine) as session:

        row = (
            session
            .query(PPCompleteDownloadFileLog)
            .filter_by(pp_complete_download_file_log_id=pp_complete_download_file_log_id)
            .one()
        )
        logger.info(f'querying database table PPCompleteDownloadFileLog for row with {pp_complete_download_file_log_id=}')

        data_decision = row.data_decision

        if data_decision == 'archive':
            logger.info(f'row with data_decision={data_decision}, run archiver')

            bucket = row.s3_tmp_bucket
            bucket_archive = 'land-registry-data-archive'
            object_key = row.s3_tmp_object_key
            logger.info(f'archive file from s3 in bucket {bucket} with object key {object_key} to bucket {bucket_archive}')

            boto3_client = (
                boto3_session.client(
                    's3',
                    endpoint_url=minio_url,
                    config=botocore.config.Config(signature_version='s3v4'),
                )
            )

            s3_copy_start_timestamp = datetime.now(timezone.utc)

            # hope this action doesn't take longer than ~ 30 seconds, or Kafka will blow up
            boto3_client.copy(
                CopySource={
                    'Bucket': bucket,
                    'Key': object_key,
                },
                Bucket=bucket_archive,
                Key=object_key,
            )

            boto3_client.delete_object(Bucket=bucket, Key=object_key)

            s3_copy_complete_timestamp = datetime.now(timezone.utc)
            s3_copy_duration = s3_copy_complete_timestamp - s3_copy_start_timestamp

            assert row.s3_archive_action_taken is None
            assert row.s3_archive_datetime is None
            assert row.s3_archive_bucket is None
            assert row.s3_archive_object_key is None
            assert row.s3_copy_start_timestamp is None
            assert row.s3_copy_duration is None

            row.s3_archive_action_taken = 'archive'
            row.s3_archive_datetime = datetime.now(timezone.utc)
            row.s3_archive_bucket = bucket_archive
            row.s3_archive_object_key = object_key
            row.s3_copy_start_timestamp = s3_copy_start_timestamp
            row.s3_copy_duration = s3_copy_duration
            session.commit()

            download_start_timestamp = row.download_start_timestamp
            data_publish_datestamp = row.data_publish_datestamp
            data_threshold_datestamp = row.data_threshold_datestamp
            data_auto_datestamp = row.data_auto_datestamp
            sha256sum = row.sha256sum

            row = PPCompleteArchiveFileLog(
                created_datetime=datetime.now(timezone.utc),
                data_source='current',
                data_download_timestamp=download_start_timestamp,
                data_publish_datestamp=data_publish_datestamp,
                data_threshold_datestamp=data_threshold_datestamp,
                data_auto_datestamp=data_auto_datestamp,
                s3_bucket=bucket_archive,
                s3_object_key=object_key,
                sha256sum=sha256sum,
            )
            session.add(row)
            session.commit()

            pp_complete_archive_file_log_id = row.pp_complete_archive_file_log_id

            notify(
                producer=producer,
                pp_complete_archive_file_log_id=pp_complete_archive_file_log_id,
            )

        elif data_decision == 'garbage_collect':
            logger.info(f'row with data_decision={data_decision}, ignore')

            assert row.s3_archive_action_taken is None
            assert row.s3_archive_datetime is None

            row.s3_archive_action_taken = 'ignore'
            row.s3_archive_datetime = datetime.now(timezone.utc)
            session.commit()

        else:
            raise RuntimeError(f'invalid data_decision {data_decision}')


def notify(
    producer: Producer,
    pp_complete_archive_file_log_id: int,
) -> None:
    logger.debug(f'sending notification')

    dto = PPCompleteArchiveNotificationDTO(
        notification_source=PROCESS_NAME_PP_COMPLETE_ARCHIVER,
        notification_type=NOTIFICATION_TYPE_PP_COMPLETE_ARCHIVE_COMPLETE,
        notification_timestamp=datetime.now(timezone.utc),
        pp_complete_archive_file_log_id=pp_complete_archive_file_log_id,
    )

    dto_json_str = jsons.dumps(dto, strip_privates=True)

    producer.produce(
        topic=TOPIC_NAME_PP_COMPLETE_ARCHIVE_NOTIFICATION,
        key=f'no_key',
        value=dto_json_str,
    )
    producer.flush()
    logger.debug(f'notification sent')


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
    logger.info(f'process exit')


