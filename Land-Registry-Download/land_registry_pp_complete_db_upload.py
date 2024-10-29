
import signal
import threading
import jsons
import io
import pandas
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

from lib_land_registry_data.lib_constants.process_name import PROCESS_NAME_PP_COMPLETE_DATABASE_UPLOADER

from lib_land_registry_data.lib_constants.topic_name import TOPIC_NAME_PP_COMPLETE_ARCHIVE_NOTIFICATION
from lib_land_registry_data.lib_constants.topic_name import TOPIC_NAME_PP_COMPLETE_DB_UPLOAD_NOTIFICATION

from lib_land_registry_data.lib_constants.notification_type import NOTIFICATION_TYPE_PP_COMPLETE_ARCHIVE_COMPLETE
from lib_land_registry_data.lib_constants.notification_type import NOTIFICATION_TYPE_PP_COMPLETE_DB_UPLOAD_COMPLETE

from lib_land_registry_data.lib_dto import PPCompleteArchiveNotificationDTO
from lib_land_registry_data.lib_dto import PPCompleteDatabaseUploadNotificationDTO

from lib_land_registry_data.lib_db import PPCompleteArchiveFileLog

from lib_land_registry_data.lib_env import EnvironmentVariables

from lib_land_registry_data.logging import set_logger_process_name
from lib_land_registry_data.logging import get_logger
from lib_land_registry_data.logging import create_stdout_log_handler
from lib_land_registry_data.logging import create_file_log_handler


event_thead_terminate = threading.Event()


set_logger_process_name(
    process_name=PROCESS_NAME_PP_COMPLETE_DATABASE_UPLOADER,
)

logger = get_logger()
stdout_log_handler = create_stdout_log_handler()
file_log_handler = create_file_log_handler(
    logger_process_name=PROCESS_NAME_PP_COMPLETE_DATABASE_UPLOADER,
    logger_file_datetime=datetime.now(timezone.utc).date(),
)
logger.addHandler(stdout_log_handler)
logger.addHandler(file_log_handler)


def main():
    logger.info(f'{PROCESS_NAME_PP_COMPLETE_DATABASE_UPLOADER} start')

    environment_variables = EnvironmentVariables()

    kafka_bootstrap_servers = environment_variables.get_kafka_bootstrap_servers()
    logger.info(f'create kafka consumer producer: bootstrap_servers={kafka_bootstrap_servers}')

    logger.info(f'create consumer')
    consumer = create_consumer(
        bootstrap_servers=kafka_bootstrap_servers,
        client_id=PROCESS_NAME_PP_COMPLETE_DATABASE_UPLOADER,
        group_id=PROCESS_NAME_PP_COMPLETE_DATABASE_UPLOADER,
    )

    logger.info(f'create producer')
    producer = create_producer(
        bootstrap_servers=kafka_bootstrap_servers,
        client_id=PROCESS_NAME_PP_COMPLETE_DATABASE_UPLOADER,
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

    logger.info(f'consumer subscribing to topic {TOPIC_NAME_PP_COMPLETE_ARCHIVE_NOTIFICATION}')
    consumer.subscribe([TOPIC_NAME_PP_COMPLETE_ARCHIVE_NOTIFICATION])
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
                PPCompleteArchiveNotificationDTO,
            )

            try:
                notification_type = dto.notification_type
                logger.debug(f'notification type: {notification_type}')

                if notification_type == NOTIFICATION_TYPE_PP_COMPLETE_ARCHIVE_COMPLETE:
                    logger.info(f'run database upload process')

                    pp_complete_archive_file_log_id = dto.pp_complete_archive_file_log_id

                    # Long running process about to start, setup consumer poll loop
                    thread_handle = threading.Thread(target=consumer_poll_loop, args=(consumer,))
                    thread_handle.start()

                    # new notification documents just trigger the garbage collection
                    # process for all old files
                    database_upload(
                        producer=producer,
                        postgres_engine=postgres_engine,
                        boto3_session=boto3_session,
                        minio_url=minio_url,
                        pp_complete_archive_file_log_id=pp_complete_archive_file_log_id,
                    )

                    event_thead_terminate.set()
                    thread_handle.join()
                    consumer.commit()
                    event_thead_terminate.clear()

                else:
                    raise RuntimeError(f'unknown notification type: {notification_type}')

            except Exception as exception:
                logger.exception(exception)

    consumer.unsubscribe()
    consumer.close()


df_columns = [
    'transaction_unique_id',
    'price',
    'transaction_date',
    'postcode',
    'property_type',
    'new_tag',
    'lease',
    'primary_address_object_name',
    'secondary_address_object_name',
    'street',
    'locality',
    'town_city',
    'district',
    'county',
    'ppd_cat',
    'record_op',
    'data_timestamp',
    'created_datetime',
]


def database_upload(
    producer: Producer,
    postgres_engine: Engine,
    boto3_session,
    minio_url: str,
    pp_complete_archive_file_log_id: int,
) -> None:
    database_upload_start_timestamp = datetime.now(timezone.utc)

    (
        s3_bucket,
        s3_object_key
    ) = get_s3_bucket_and_object_key_from_database(
        postgres_engine=postgres_engine,
        pp_complete_archive_file_log_id=pp_complete_archive_file_log_id,
    )

    logger.info(f's3_bucket={s3_bucket}, s3_object_key={s3_object_key}')

    boto3_client = (
        boto3_session.client(
            's3',
            endpoint_url=minio_url,
            config=botocore.config.Config(signature_version='s3v4'),
        )
    )

    # TODO: change to debug
    logger.info(f'downloading data from s3 bucket')
    s3_response_object = boto3_client.get_object(Bucket=s3_bucket, Key=s3_object_key)
    pp_monthly_update_data = s3_response_object['Body'].read()

    logger.info(f'load pandas dataframe')
    df = pandas.read_csv(
        io.BytesIO(pp_monthly_update_data),
        header=None,
    )

    data_timestamp = get_data_timestamp_from_database(
        postgres_engine=postgres_engine,
        pp_complete_archive_file_log_id=pp_complete_archive_file_log_id,
    )
    df['data_timestamp'] = data_timestamp

    created_datetime = datetime.now(timezone.utc)
    df['created_datetime'] = created_datetime

    df.columns = df_columns

    logger.info(f'load sql database')
    df.to_sql(
        name='pp_complete_data',
        schema='land_registry_2',
        con=postgres_engine,
        if_exists='replace',
        index=False,
        chunksize=1000,
    )

    database_upload_complete_timestamp = datetime.now(timezone.utc)
    database_upload_duration = database_upload_complete_timestamp - database_upload_start_timestamp

    with Session(postgres_engine) as session:
        row = (
            session
            .query(PPCompleteArchiveFileLog)
            .filter_by(pp_complete_archive_file_log_id=pp_complete_archive_file_log_id)
            .one()
        )

        assert row.database_upload_start_timestamp is None
        assert row.database_upload_duration is None

        row.database_upload_start_timestamp = database_upload_start_timestamp
        row.database_upload_duration = database_upload_duration
        session.commit()

    logger.info(f'load sql database complete')
    notify(
        producer=producer,
        pp_complete_archive_file_log_id=pp_complete_archive_file_log_id,
    )


def get_s3_bucket_and_object_key_from_database(
    postgres_engine: Engine,
    pp_complete_archive_file_log_id: int,
) -> tuple[str, str]:
    with Session(postgres_engine) as session:
        row = (
            session
            .query(PPCompleteArchiveFileLog)
            .filter_by(pp_complete_archive_file_log_id=pp_complete_archive_file_log_id)
            .one()
        )
        s3_bucket = row.s3_bucket
        s3_object_key = row.s3_object_key
        return (s3_bucket, s3_object_key)


def get_data_timestamp_from_database(
    postgres_engine: Engine,
    pp_complete_archive_file_log_id: int,
) -> int:
    with Session(postgres_engine) as session:
        row = (
            session
            .query(PPCompleteArchiveFileLog)
            .filter_by(pp_complete_archive_file_log_id=pp_complete_archive_file_log_id)
            .one()
        )
        data_timestamp = row.data_threshold_datestamp
        return data_timestamp


def notify(
    producer: Producer,
    pp_complete_archive_file_log_id: int,
) -> None:
    logger.debug(f'sending notification')

    dto = PPCompleteDatabaseUploadNotificationDTO(
        notification_source=PROCESS_NAME_PP_COMPLETE_DATABASE_UPLOADER,
        notification_type=NOTIFICATION_TYPE_PP_COMPLETE_DB_UPLOAD_COMPLETE,
        notification_timestamp=datetime.now(timezone.utc),
        pp_complete_archive_file_log_id=pp_complete_archive_file_log_id,
    )

    dto_json_str = jsons.dumps(dto, strip_privates=True)

    producer.produce(
        topic=TOPIC_NAME_PP_COMPLETE_DB_UPLOAD_NOTIFICATION,
        key=f'no_key',
        value=dto_json_str,
    )
    producer.flush()
    logger.debug(f'notification sent')


def consumer_poll_loop(consumer: Consumer) -> None:
    logger.debug('consumer_poll_loop: consumer_poll_loop starts')

    logger.debug('consumer_poll_loop: pausing consumer')
    topic_partition_assignment = consumer.assignment()
    consumer.pause(topic_partition_assignment)
    logger.debug('consumer_poll_loop: consumer paused')

    while True:
        consumer_short_poll_duration = 1.0
        message = consumer.poll(consumer_short_poll_duration)

        if message is not None:
            raise RuntimeError(f'consumer abort')

        if event_thead_terminate.is_set():
            logger.debug(f'consumer_poll_loop: event set')
            break

    logger.debug(f'consumer_poll_loop: resuming consumer')
    consumer.resume(topic_partition_assignment)
    logger.debug(f'consumer_poll_loop: consumer resumed')


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

