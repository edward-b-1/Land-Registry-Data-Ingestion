
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

from lib_land_registry_data.lib_constants.process_name import PROCESS_NAME_PP_MONTHLY_UPDATE_DATABASE_UPDATER

from lib_land_registry_data.lib_constants.topic_name import TOPIC_NAME_PP_MONTHLY_UPDATE_ARCHIVE_NOTIFICATION
from lib_land_registry_data.lib_constants.topic_name import TOPIC_NAME_PP_MONTHLY_UPDATE_DB_UPDATE_NOTIFICATION

from lib_land_registry_data.lib_constants.notification_type import NOTIFICATION_TYPE_PP_MONTHLY_UPDATE_ARCHIVE_COMPLETE
from lib_land_registry_data.lib_constants.notification_type import NOTIFICATION_TYPE_PP_MONTHLY_UPDATE_DB_UPDATE_COMPLETE

from lib_land_registry_data.lib_dto import PPMonthlyUpdateArchiveNotificationDTO
from lib_land_registry_data.lib_dto import PPMonthlyUpdateDatabaseUpdateNotificationDTO

from lib_land_registry_data.lib_db import PPMonthlyUpdateArchiveFileLog
from lib_land_registry_data.lib_db import PPMonthlyData

from lib_land_registry_data.lib_env import EnvironmentVariables

from lib_land_registry_data.logging import set_logger_process_name
from lib_land_registry_data.logging import get_logger
from lib_land_registry_data.logging import create_stdout_log_handler
from lib_land_registry_data.logging import create_file_log_handler


event_thead_terminate = threading.Event()


set_logger_process_name(
    process_name=PROCESS_NAME_PP_MONTHLY_UPDATE_DATABASE_UPDATER,
)

logger = get_logger()
stdout_log_handler = create_stdout_log_handler()
file_log_handler = create_file_log_handler(
    logger_process_name=PROCESS_NAME_PP_MONTHLY_UPDATE_DATABASE_UPDATER,
    logger_file_datetime=datetime.now(timezone.utc).date(),
)
logger.addHandler(stdout_log_handler)
logger.addHandler(file_log_handler)


def main():
    logger.info(f'{PROCESS_NAME_PP_MONTHLY_UPDATE_DATABASE_UPDATER} start')

    environment_variables = EnvironmentVariables()

    kafka_bootstrap_servers = environment_variables.get_kafka_bootstrap_servers()
    logger.info(f'create kafka consumer producer: bootstrap_servers={kafka_bootstrap_servers}')

    logger.info(f'create consumer')
    consumer = create_consumer(
        bootstrap_servers=kafka_bootstrap_servers,
        client_id=PROCESS_NAME_PP_MONTHLY_UPDATE_DATABASE_UPDATER,
        group_id=PROCESS_NAME_PP_MONTHLY_UPDATE_DATABASE_UPDATER,
    )

    logger.info(f'create producer')
    producer = create_producer(
        bootstrap_servers=kafka_bootstrap_servers,
        client_id=PROCESS_NAME_PP_MONTHLY_UPDATE_DATABASE_UPDATER,
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

    logger.info(f'consumer subscribing to topic {TOPIC_NAME_PP_MONTHLY_UPDATE_ARCHIVE_NOTIFICATION}')
    consumer.subscribe([TOPIC_NAME_PP_MONTHLY_UPDATE_ARCHIVE_NOTIFICATION])
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
                PPMonthlyUpdateArchiveNotificationDTO,
            )

            try:
                notification_type = dto.notification_type
                logger.debug(f'notification type: {notification_type}')

                if notification_type == NOTIFICATION_TYPE_PP_MONTHLY_UPDATE_ARCHIVE_COMPLETE:
                    logger.info(f'run database upload process')

                    pp_monthly_update_archive_file_log_id = dto.pp_monthly_update_archive_file_log_id
                    logger.info(f'{pp_monthly_update_archive_file_log_id=}')

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
                        pp_monthly_update_archive_file_log_id=pp_monthly_update_archive_file_log_id,
                    )

                    event_thead_terminate.set()
                    thread_handle.join()
                    consumer.commit()
                    event_thead_terminate.clear()

                else:
                    raise RuntimeError(f'unknown notification type: {notification_type}')

            except Exception as exception:
                log_message = f'notification error: {exception}'
                logger.error(log_message)

    consumer.unsubscribe()
    consumer.close()


df_columns_1 = [
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
]

df_columns_2 = [
    'insert_op_count',
    'update_op_count',
    'delete_op_count',
    'created_datetime',
    'inserted_datetime',
    'updated_datetime',
    'deleted_datetime',
]


def database_upload(
    producer: Producer,
    postgres_engine: Engine,
    boto3_session,
    minio_url: str,
    pp_monthly_update_archive_file_log_id: int,
) -> None:
    datetime_now = datetime.now(timezone.utc)
    database_upload_start_timestamp = datetime_now

    (
        s3_bucket,
        s3_object_key,
    ) = get_s3_bucket_and_object_key_from_database(
        postgres_engine=postgres_engine,
        pp_monthly_update_archive_file_log_id=pp_monthly_update_archive_file_log_id,
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

    logger.info(f'number of dataframe columns: {len(df.columns)}')
    logger.info(f'dataframe columns:')
    for column in df.columns:
        logger.info(f'{column}')

    if len(df.columns) == 16:
        pass
    elif len(df.columns) == 15:
        df['ppd_cat'] = None
    else:
        raise RuntimeError(f'invalid number of columns: {len(df.columns)}')

    data_timestamp = get_data_timestamp_from_database(
        postgres_engine=postgres_engine,
        pp_monthly_update_archive_file_log_id=pp_monthly_update_archive_file_log_id,
    )
    df['data_timestamp'] = data_timestamp

    df.columns = df_columns_1

    logger.info(f'starting database upload operation')
    with Session(postgres_engine) as session:

        for index, row in df.iterrows():
            transaction_unique_id = row['transaction_unique_id']
            record_op = row['record_op']

            if record_op == 'A':
                pass
            elif record_op == 'C':
                pass
            elif record_op == 'D':
                pass
            else:
                raise RuntimeError(f'invalid {record_op=}')

            existing_row = (
                session
                .query(PPMonthlyData)
                .filter_by(transaction_unique_id=transaction_unique_id)
                .filter_by(is_deleted=False)
                .one_or_none()
            )

            if existing_row is None:
                if record_op == 'C':
                    raise RuntimeError(f'{record_op=} when existing_row is None')
                elif record_op == 'D':
                    raise RuntimeError(f'{record_op=} when existing_row is None')
                else:
                    if record_op == 'A':
                        row = do_operation_add_row(
                            existing_row=existing_row,
                            new_row_data=row,
                        )
                        session.add(row)

            else:
                if record_op == 'A':
                    raise RuntimeError(f'{record_op=} when existing_row is not None')
                else:
                    if record_op == 'C':
                        do_operation_change_row(
                            existing_row=existing_row,
                            new_row_data=row,
                        )
                    elif record_op == 'D':
                        do_operation_delete_row(
                            existing_row=existing_row,
                            datetime_now=datetime_now
                        )

    database_upload_monthly_update_timestamp = datetime.now(timezone.utc)
    database_upload_duration = database_upload_monthly_update_timestamp - database_upload_start_timestamp
    logger.info(f'database upload operation complete')

    logger.info(f'update table PPMonthlyUpdateArchiveFileLog')
    with Session(postgres_engine) as session:
        row = (
            session
            .query(PPMonthlyUpdateArchiveFileLog)
            .filter_by(pp_monthly_update_archive_file_log_id=pp_monthly_update_archive_file_log_id)
            .one()
        )

        # added by mistake - remove?
        assert row.database_update_start_timestamp is None
        assert row.database_update_duration is None

        row.database_update_start_timestamp = database_upload_start_timestamp
        row.database_update_duration = database_upload_duration
        session.commit()
    logger.info('updated table PPMonthlyUpdateArchiveFileLog')

    logger.info(f'sql database update complete')
    notify(
        producer=producer,
        pp_monthly_update_archive_file_log_id=pp_monthly_update_archive_file_log_id,
    )


def do_operation_add_row(
    new_row_data,
    datetime_now: datetime,
) -> PPMonthlyData:
    row = PPMonthlyData(
        transaction_unique_id=new_row_data['transaction_unique_id'],
        price=new_row_data['price'],
        transaction_date = new_row_data['transaction_date'],
        postcode = new_row_data['postcode'],
        property_type = new_row_data['property_type'],
        new_tag = new_row_data['new_tag'],
        lease = new_row_data['lease'],
        primary_address_object_name = new_row_data['primary_address_object_name'],
        secondary_address_object_name = new_row_data['secondary_address_object_name'],
        street = new_row_data['street'],
        locality = new_row_data['locality'],
        town_city = new_row_data['town_city'],
        district = new_row_data['district'],
        county = new_row_data['county'],
        ppd_cat = new_row_data['ppd_cat'],
        record_op = new_row_data['record_op'],
        data_timestamp = new_row_data['data_timestamp'],

        insert_op_count=1,
        update_op_count=0,
        delete_op_count=0,
        created_datetime=datetime_now,
        inserted_datetime=datetime_now,
        updated_datetime=None,
        deleted_datetime=None,
        is_deleted=False,
    )
    return row


def do_operation_change_row(
    existing_row: PPMonthlyData,
    new_row_data,
    datetime_now: datetime,
) -> None:
    existing_row.price = new_row_data['price']
    existing_row.transaction_date = new_row_data['transaction_date']
    existing_row.postcode = new_row_data['postcode']
    existing_row.property_type = new_row_data['property_type']
    existing_row.new_tag = new_row_data['new_tag']
    existing_row.lease = new_row_data['lease']
    existing_row.primary_address_object_name = new_row_data['primary_address_object_name']
    existing_row.secondary_address_object_name = new_row_data['secondary_address_object_name']
    existing_row.street = new_row_data['street']
    existing_row.locality = new_row_data['locality']
    existing_row.town_city = new_row_data['town_city']
    existing_row.district = new_row_data['district']
    existing_row.county = new_row_data['county']
    existing_row.ppd_cat = new_row_data['ppd_cat']
    existing_row.record_op = new_row_data['record_op']
    existing_row.data_datestamp = new_row_data['data_timestamp']

    existing_row.update_op_count += 1
    existing_row.updated_datetime = datetime_now


def do_operation_delete_row(
    existing_row: PPMonthlyData,
    datetime_now: datetime,
) -> None:
    existing_row.is_deleted = True

    existing_row.delete_op_count += 1
    existing_row.deleted_datetime = datetime_now


def get_s3_bucket_and_object_key_from_database(
    postgres_engine: Engine,
    pp_monthly_update_archive_file_log_id: int,
) -> tuple[str, str]:
    with Session(postgres_engine) as session:
        row = (
            session
            .query(PPMonthlyUpdateArchiveFileLog)
            .filter_by(pp_monthly_update_archive_file_log_id=pp_monthly_update_archive_file_log_id)
            .one()
        )
        s3_bucket = row.s3_bucket
        s3_object_key = row.s3_object_key
        return (s3_bucket, s3_object_key)


def get_data_timestamp_from_database(
    postgres_engine: Engine,
    pp_monthly_update_archive_file_log_id: int,
) -> int:
    with Session(postgres_engine) as session:
        row = (
            session
            .query(PPMonthlyUpdateArchiveFileLog)
            .filter_by(pp_monthly_update_archive_file_log_id=pp_monthly_update_archive_file_log_id)
            .one()
        )
        data_timestamp = row.data_threshold_datestamp
        return data_timestamp


def notify(
    producer: Producer,
    pp_monthly_update_archive_file_log_id: int,
) -> None:
    logger.debug(f'sending notification')

    dto = PPMonthlyUpdateDatabaseUpdateNotificationDTO(
        notification_source=PROCESS_NAME_PP_MONTHLY_UPDATE_DATABASE_UPDATER,
        notification_type=NOTIFICATION_TYPE_PP_MONTHLY_UPDATE_DB_UPDATE_COMPLETE,
        notification_timestamp=datetime.now(timezone.utc),
        pp_monthly_update_archive_file_log_id=pp_monthly_update_archive_file_log_id,
    )

    dto_json_str = jsons.dumps(dto, strip_privates=True)

    producer.produce(
        topic=TOPIC_NAME_PP_MONTHLY_UPDATE_DB_UPDATE_NOTIFICATION,
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

