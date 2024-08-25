
import time
import signal
import threading
import hashlib
import jsons
import requests
from datetime import datetime
from datetime import date
from datetime import timedelta
from datetime import timezone

from dataclasses import dataclass

from confluent_kafka import Consumer
from confluent_kafka import Producer

from sqlalchemy import Engine
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import boto3
import botocore

from lib_land_registry_data.lib_constants.process_name import PROCESS_NAME_PP_COMPLETE_DOWNLOADER

from lib_land_registry_data.lib_topic_name import TOPIC_NAME_CRON_TRIGGER_NOTIFICATION
from lib_land_registry_data.lib_topic_name import TOPIC_NAME_PP_COMPLETE_DOWNLOAD_NOTIFICATION

from lib_land_registry_data.lib_constants.notification_type import NOTIFICATION_TYPE_CRON_TRIGGER
from lib_land_registry_data.lib_constants.notification_type import NOTIFICATION_TYPE_PP_COMPLETE_DOWNLOAD_COMPLETE

from lib_land_registry_data.lib_kafka import create_consumer
from lib_land_registry_data.lib_kafka import create_producer

from lib_land_registry_data.lib_dto import CronTriggerNotificationDTO
from lib_land_registry_data.lib_dto import PPCompleteDownloadCompleteNotificationDTO

from lib_land_registry_data.lib_env import EnvironmentVariables

from lib_land_registry_data.logging import set_logger_process_name
from lib_land_registry_data.logging import get_logger
from lib_land_registry_data.logging import create_stdout_log_handler
from lib_land_registry_data.logging import create_file_log_handler

from lib_land_registry_data.lib_db import PPCompleteDownloadFileLog


event_thead_terminate = threading.Event()


set_logger_process_name(
    process_name=PROCESS_NAME_PP_COMPLETE_DOWNLOADER,
)

logger = get_logger()
stdout_log_handler = create_stdout_log_handler()
file_log_handler = create_file_log_handler(
    logger_process_name=PROCESS_NAME_PP_COMPLETE_DOWNLOADER,
    logger_file_datetime=datetime.now(timezone.utc).date(),
)
logger.addHandler(stdout_log_handler)
logger.addHandler(file_log_handler)


def main():
    logger.info(f'{PROCESS_NAME_PP_COMPLETE_DOWNLOADER} start')

    environment_variables = EnvironmentVariables()

    kafka_bootstrap_servers = environment_variables.get_kafka_bootstrap_servers()
    logger.info(f'create kafka consumer producer: bootstrap_servers={kafka_bootstrap_servers}')

    consumer = create_consumer(
        bootstrap_servers=kafka_bootstrap_servers,
        client_id=PROCESS_NAME_PP_COMPLETE_DOWNLOADER,
        group_id=PROCESS_NAME_PP_COMPLETE_DOWNLOADER,
    )

    producer = create_producer(
        bootstrap_servers=kafka_bootstrap_servers,
        client_id=PROCESS_NAME_PP_COMPLETE_DOWNLOADER,
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

    logger.info(f'create database engine: postgres_host={environment_variables.get_postgres_host()}')
    postgres_connection_string = environment_variables.get_postgres_connection_string()
    engine_postgres = create_engine(postgres_connection_string)

    kafka_event_loop(
        consumer=consumer,
        producer=producer,
        boto3_session=boto3_session,
        minio_url=minio_url,
        engine_postgres=engine_postgres,
    )


def kafka_event_loop(
    consumer: Consumer,
    producer: Producer,
    boto3_session,
    minio_url: str,
    engine_postgres: Engine
) -> None:

    logger.info(f'consumer subscribing to topic {TOPIC_NAME_CRON_TRIGGER_NOTIFICATION}')
    consumer.subscribe([TOPIC_NAME_CRON_TRIGGER_NOTIFICATION])
    consumer_poll_timeout = 5.0
    logger.info(f'consumer poll timeout: {consumer_poll_timeout}')
    message_queue = []

    global exit_flag

    while not exit_flag:

        # provide a short period flush to allow the producer to catch up
        # with dispatched events
        producer.flush(3.0)
        producer.poll(1.0)
        message = consumer.poll(consumer_poll_timeout)

        if message is None:
            if len(message_queue) > 0:
                process_message_queue(
                    message_queue=message_queue,
                    consumer=consumer,
                    producer=producer,
                    boto3_session=boto3_session,
                    minio_url=minio_url,
                    engine_postgres=engine_postgres,
                )
                message_queue.clear()
            continue

        if message.error():
            logger.error(f'kafka message error: {message.value().decode()}')
            raise RuntimeError(f'{message.value().decode()}')
        else:
            logger.debug(f'message received')
            document = jsons.loads(
                message.value().decode(),
                CronTriggerNotificationDTO,
            )

            try:
                notification_type = document.notification_type
                logger.debug(f'message type {notification_type}')

                if notification_type == NOTIFICATION_TYPE_CRON_TRIGGER:
                    logger.debug(f'appending message of type {notification_type} to message queue')
                    message_queue.append(document)

                else:
                    raise RuntimeError(f'unknown notification type: {notification_type}')

            except Exception as exception:
                logger.error(f'notification error: {exception}')

    consumer.unsubscribe()
    consumer.close()


def process_message_queue(
    message_queue: list,
    consumer: Consumer,
    producer: Producer,
    boto3_session,
    minio_url: str,
    engine_postgres: Engine,
):
    # TODO: can raise exception (but doesn't due to logic elsewhere)
    (
        run_download_flag,
        pp_complete_file_log_id,
    ) = process_message_queue_filter_for_download_trigger_message(message_queue)

    if run_download_flag:
        # Long running process about to start, setup consumer poll loop
        thread_handle = threading.Thread(target=consumer_poll_loop, args=(consumer,))
        thread_handle.start()

        cron_target_date = get_cron_target_date_from_database(
            pp_complete_file_log_id=pp_complete_file_log_id,
            engine_postgres=engine_postgres,
        )

        run_download_and_update_database_and_notify(
            pp_complete_file_log_id=pp_complete_file_log_id,
            cron_target_date=cron_target_date,
            producer=producer,
            boto3_session=boto3_session,
            minio_url=minio_url,
            engine_postgres=engine_postgres,
        )

        event_thead_terminate.set()
        thread_handle.join()
        consumer.commit()
        event_thead_terminate.clear()


def process_message_queue_filter_for_download_trigger_message(
    message_queue: list[CronTriggerNotificationDTO],
) -> tuple[bool, int|None]:
    # filter out any messages which are not of the correct type to prevent
    # message = message_queue[-1]
    # from retrieving a message which is not of the correct type
    def filter_messages_by_type(message: CronTriggerNotificationDTO) -> bool:
        notification_type = message.notification_type
        if notification_type == NOTIFICATION_TYPE_CRON_TRIGGER:
            return True
        return False

    message_queue = (
        list(
            filter(
                filter_messages_by_type,
                message_queue,
            )
        )
    )

    if len(message_queue) > 0:
        message = message_queue[-1]

        notification_type = message.notification_type
        if notification_type == NOTIFICATION_TYPE_CRON_TRIGGER:
            return (True, message.pp_complete_file_log_id)
        else:
            raise RuntimeError(f'unknown notification type: {notification_type}')
    else:
        return (False, None)


def get_cron_target_date_from_database(
    pp_complete_file_log_id: int,
    engine_postgres: Engine,
) -> date:
    with Session(engine_postgres) as session:
        row = (
            session
            .query(PPCompleteDownloadFileLog)
            .filter_by(pp_complete_file_log_id=pp_complete_file_log_id)
            .one()
        )
        cron_target_date = row.cron_target_date
        return cron_target_date


def run_download_and_update_database_and_notify(
    pp_complete_file_log_id: int,
    cron_target_date: date,
    producer: Producer,
    boto3_session,
    minio_url: str,
    engine_postgres: Engine,
):
    logger.info('run_download_process: run_download_and_notify')
    (
        success,
        download_upload_statistics,
        hash_statistics,
    ) = download_pp_complete_and_upload_to_s3(
        cron_target_date=cron_target_date,
        pp_complete_file_log_id=pp_complete_file_log_id,
        engine_postgres=engine_postgres,
        boto3_session=boto3_session,
        minio_url=minio_url,
    )

    if success:
        notify(
            producer=producer,
            pp_complete_file_log_id=pp_complete_file_log_id,
        )
    else:
        logger.error(f'failed to download file, give up, will not try again')


@dataclass
class DownloadUploadStatistics():
    download_start_timestamp: datetime
    download_complete_timestamp: datetime
    download_duration: timedelta
    s3_upload_start_timestamp: datetime
    s3_upload_complete_timestamp: datetime
    s3_upload_duration: timedelta
    s3_bucket: str
    s3_object_key: str

@dataclass
class HashStatistics():
    hash_start_timestamp: datetime
    hash_complete_timestamp: datetime
    hash_duration: timedelta
    hash_hex_str: str


def download_pp_complete_and_upload_to_s3(
    cron_target_date: date,
    pp_complete_file_log_id: int,
    engine_postgres: Engine,
    boto3_session,
    minio_url: str,
) -> tuple[bool, DownloadUploadStatistics|None, HashStatistics|None]:
    url = 'http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.txt'

    fail_count = 0
    while True:
        try:
            (
                data,
                download_start_timestamp,
                download_complete_timestamp,
                download_duration,
            ) = download_data_to_memory(url)

        except Exception as error:
            logger.error(f'{error}')

            fail_count += 1
            if fail_count > 20:
                logger.error(f'download failed after {fail_count} retries, give up')
                return (False, None, None)
            else:
                logger.warning(f'download failed, retry in 1h, number of failures: {fail_count}')
                time_1_hour = 3600
                time.sleep(time_1_hour)

        (
            s3_object_key,
            s3_upload_start_timestamp,
            s3_upload_complete_timestamp,
            s3_upload_duration,
        ) = upload_data_to_s3(data, cron_target_date, boto3_session, minio_url)

        download_upload_statistics = DownloadUploadStatistics(
            download_start_timestamp=download_start_timestamp,
            download_complete_timestamp=download_complete_timestamp,
            download_duration=download_duration,
            s3_upload_start_timestamp=s3_upload_start_timestamp,
            s3_upload_complete_timestamp=s3_upload_complete_timestamp,
            s3_upload_duration=s3_upload_duration,
            s3_bucket='land-registry-data-tmp',
            s3_object_key=s3_object_key,
        )

        update_database_s3(
            engine_postgres=engine_postgres,
            pp_complete_file_log_id=pp_complete_file_log_id,
            download_upload_statistics=download_upload_statistics,
        )

        (
            hash_start_timestamp,
            hash_complete_timestamp,
            hash_duration,
            sha256sum_hex_str,
        ) = calculate_sha256sum(data)

        hash_statistics = HashStatistics(
            hash_start_timestamp=hash_start_timestamp,
            hash_complete_timestamp=hash_complete_timestamp,
            hash_duration=hash_duration,
            hash_hex_str=sha256sum_hex_str,
        )

        update_database_sha256sum(
            engine_postgres=engine_postgres,
            pp_complete_file_log_id=pp_complete_file_log_id,
            hash_statistics=hash_statistics,
        )

        return (True, download_upload_statistics, hash_statistics)


def download_data_to_memory(
    url: str,
) -> tuple[bytes, datetime, datetime, timedelta]|None:

    download_start_timestamp = datetime.now(timezone.utc)

    logger.info(f'downloading from {url}: download starting {download_start_timestamp}')

    response = requests.get(url, allow_redirects=True)
    if response.status_code == 200:
        logger.info(f'status_code={response.status_code}')
    else:
        logger.error(f'status_code={response.status_code}')
        download_complete_timestamp = datetime.now(timezone.utc)
        download_duration = download_complete_timestamp - download_start_timestamp
        logger.error(f'download failure: {download_duration}')
        raise RuntimeError(f'request failure {response.status_code}')

    # Since we have the data here, why not calculate the shasum of it?
    data = response.content

    download_complete_timestamp = datetime.now(timezone.utc)
    logger.info(f'download complete: {download_complete_timestamp}')
    download_duration = download_complete_timestamp - download_start_timestamp
    logger.info(f'download duration: {download_duration}')

    return (data, download_start_timestamp, download_complete_timestamp, download_duration)


def upload_data_to_s3(
    data: bytes,
    cron_target_date: date,
    boto3_session,
    minio_url: str,
) -> tuple[str, datetime, datetime, timedelta]|None:

    bucket = 'land-registry-data-tmp'
    object_key = f'pp-complete-{cron_target_date}.txt'

    # check if the object key exists - if it does, raise Exception (?)
    boto3_client = (
        boto3_session.client(
            's3',
            endpoint_url=minio_url,
            config=botocore.config.Config(signature_version='s3v4'),
        )
    )
    try:
        if boto3_client.head_object(Bucket=bucket, Key=object_key):
            exists=True
            raise RuntimeError(f'file exists in S3 storage: bucket={bucket} key={object_key}')
        else:
            exists=False
            pass
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == '404':
            exists=False
            pass
        else:
            raise

    s3_upload_start_timestamp = datetime.now(timezone.utc)
    logger.info(f'uploading to s3: upload starting {s3_upload_start_timestamp}')
    boto3_client.put_object(Bucket=bucket, Key=object_key, Body=data)
    s3_upload_complete_timestamp = datetime.now(timezone.utc)
    logger.info(f'upload complete: {s3_upload_complete_timestamp}')
    s3_upload_duration = s3_upload_complete_timestamp - s3_upload_start_timestamp
    logger.info(f'upload duration: {s3_upload_duration}')

    return (object_key, s3_upload_start_timestamp, s3_upload_complete_timestamp, s3_upload_duration)


def update_database_s3(
    engine_postgres: Engine,
    pp_complete_file_log_id: int,
    download_upload_statistics: DownloadUploadStatistics
) -> None:
    with Session(engine_postgres) as session:
        row = (
            session
            .query(PPCompleteDownloadFileLog)
            .filter_by(pp_complete_file_log_id=pp_complete_file_log_id)
            .one()
        )

        row.download_start_timestamp = download_upload_statistics.download_start_timestamp
        row.download_duration = download_upload_statistics.download_duration
        row.s3_tmp_bucket = download_upload_statistics.s3_bucket
        row.s3_tmp_object_key = download_upload_statistics.s3_object_key
        row.s3_upload_to_tmp_bucket_start_timestamp = download_upload_statistics.s3_upload_start_timestamp
        row.s3_upload_to_tmp_bucket_duration = download_upload_statistics.s3_upload_duration

        session.commit()


def update_database_sha256sum(
    engine_postgres: Engine,
    pp_complete_file_log_id: int,
    hash_statistics: HashStatistics,
) -> None:
    with Session(engine_postgres) as session:
        row = (
            session
            .query(PPCompleteDownloadFileLog)
            .filter_by(pp_complete_file_log_id=pp_complete_file_log_id)
            .one()
        )

        row.sha256sum_start_timestamp = hash_statistics.hash_start_timestamp
        row.sha256sum_duration = hash_statistics.hash_duration
        row.sha256sum = hash_statistics.hash_hex_str

        session.commit()


def calculate_sha256sum(data: bytes) -> tuple[datetime, datetime, timedelta, str]:
    hash_start_timestamp = datetime.now(timezone.utc)
    logger.info(f'sha256: calculation starting {hash_start_timestamp}')
    hash = hashlib.sha256(data).hexdigest()
    hash_complete_timestamp = datetime.now(timezone.utc)
    logger.info(f'sha256 calculation complete: {hash_complete_timestamp}')
    hash_duration = hash_complete_timestamp - hash_start_timestamp
    logger.info(f'sha256 calculation duration: {hash_duration}')
    logger.info(f'sha256sum: {hash}')
    return (hash_start_timestamp, hash_complete_timestamp, hash_duration, hash)


def notify(
    producer: Producer,
    pp_complete_file_log_id: int,
) -> None:

    document = PPCompleteDownloadCompleteNotificationDTO(
        notification_source=PROCESS_NAME_PP_COMPLETE_DOWNLOADER,
        notification_type=NOTIFICATION_TYPE_PP_COMPLETE_DOWNLOAD_COMPLETE,
        notification_timestamp=datetime.now(timezone.utc),
        pp_complete_file_log_id=pp_complete_file_log_id,
    )

    document_json_str = jsons.dumps(document, strip_privates=True)

    producer.produce(
        topic=TOPIC_NAME_PP_COMPLETE_DOWNLOAD_NOTIFICATION,
        key=f'no_key',
        value=document_json_str,
    )
    producer.flush()


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
