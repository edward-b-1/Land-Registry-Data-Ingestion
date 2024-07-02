#!/usr/bin/env python3

import jsons
from datetime import datetime
from datetime import timedelta
from datetime import timezone

import os
import signal
import threading
import pandas

from lib_land_registry_download.lib_kafka import create_consumer
from lib_land_registry_download.lib_kafka import create_producer

from confluent_kafka import Consumer
from confluent_kafka import Producer

from lib_land_registry_download.lib_topic_name import topic_name_land_registry_download_monthly_update_data_decision_notification
from lib_land_registry_download.lib_topic_name import topic_name_land_registry_download_monthly_update_database_updater_notification

from lib_land_registry_download.lib_dto import MonthlyUpdateDataDecisionCompleteNotificationDTO
from lib_land_registry_download.lib_dto import MonthlyUpdateDatabaseUpdateCompleteNotificationDTO

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from lib_land_registry_download.lib_db import PricePaidDataMonthlyUpdateFileLog
from lib_land_registry_download.lib_db import PricePaidData
from lib_land_registry_download.lib_db import PricePaidDataLog
from lib_land_registry_download.lib_db import PricePaidDataMonthlyUpdateDatabaseUpdaterOperationLog

import logging
import sys

from lib_land_registry_download.lib_constants.process_name import PROCESS_NAME_LAND_REGISTRY_MONTHLY_UPDATE_DATABASE_UPDATER as PROCESS_NAME
from lib_land_registry_download.lib_constants.process_name import OLD_PROCESS_NAME_LAND_REGISTRY_MONTHLY_UPDATE_DATABASE_UPDATER as GROUP_ID
from lib_land_registry_download.lib_constants.process_name import OLD_PROCESS_NAME_LAND_REGISTRY_MONTHLY_UPDATE_DATABASE_UPDATER as CLIENT_ID
from lib_land_registry_download.lib_constants.process_name import PROCESS_NAME_LAND_REGISTRY_MONTHLY_UPDATE_DATA_DECISION
from lib_land_registry_download.lib_constants.process_name import OLD_PROCESS_NAME_LAND_REGISTRY_MONTHLY_UPDATE_DATA_DECISION
from lib_land_registry_download.lib_constants.notification_type import DAILY_DOWNLOAD_MONTHLY_UPDATE_DATA_DECISION_COMPLETE
from lib_land_registry_download.lib_constants.notification_type import DAILY_DOWNLOAD_MONTHLY_UPDATE_DATABASE_UPDATE_COMPLETE


event_thead_terminate = threading.Event()


from dataclasses import dataclass

@dataclass
class InputFileStatistics:
    # number of rows in the input file to process
    input_file_row_count: int
    # count of number of rows in input file marked with 'A', 'C' or 'D'
    input_file_row_count_marked_as_add: int
    input_file_row_count_marked_as_change: int
    input_file_row_count_marked_as_delete: int

    # number of rows marked to be added which did not exist
    input_file_row_count_marked_as_add_and_added: int
    # number of rows marked to be added which already existed with correct data
    input_file_row_count_marked_as_add_but_already_identical_and_ignored: int
    # number of rows marked to be added which tuid existed, but data was not correct so was changed
    input_file_row_count_marked_as_add_but_changed: int
    # number of rows marked to be added which tuid exists but is marked as deleted
    input_file_row_count_marked_as_add_but_deleted_and_ignored: int

    # number of rows marked to be changed, which tuid existed but data was different so changed
    input_file_row_count_marked_as_change_and_changed: int
    # number of rows marked to be changed, which tuid existed, but data existed with identical data
    input_file_row_count_marked_as_change_but_already_identical_and_ignored: int
    # number of row marked to be changed, but tuid did not exist, so added
    input_file_row_count_marked_as_change_but_missing_and_added: int
    # number of rows marked to be changed which tuid exists but is marked as deleted
    input_file_row_count_marked_as_change_but_deleted_and_ignored: int

    # number of rows marked as deleted, which tuid existed, and identical, so deleted
    input_file_row_count_marked_as_delete_and_deleted: int
    # number of rows marked as deleted, which tuid existed, but not identical, so changed and deleted
    input_file_row_count_marked_as_delete_but_not_identical_and_changed_and_deleted: int
    # number of rows marked as deleted, but missing, so noop
    input_file_row_count_marked_as_delete_but_missing_and_ignored: int
    # number of rows marked to be deleted which tuid exists but is marked as deleted
    input_file_row_count_marked_as_delete_but_deleted_and_ignored: int



def get_log(
    PROCESS_NAME: str,
):
    log = logging.getLogger(__name__)

    stdout_log_formatter = logging.Formatter(
        '%(name)s: %(asctime)s | %(levelname)s | %(filename)s:%(lineno)s | %(process)d | %(message)s'
    )

    stdout_log_handler = logging.StreamHandler(stream=sys.stdout)
    stdout_log_handler.setLevel(logging.INFO)
    stdout_log_handler.setFormatter(stdout_log_formatter)

    file_log_formatter = logging.Formatter(
        '%(name)s: %(asctime)s | %(levelname)s | %(filename)s:%(lineno)s | %(process)d | %(message)s'
    )

    file_log_handler = logging.FileHandler(
        filename=f'{PROCESS_NAME}_{datetime.now(timezone.utc).date()}.log'
    )
    file_log_handler.setLevel(logging.INFO)
    file_log_handler.setFormatter(file_log_formatter)

    log.setLevel(logging.DEBUG)
    log.addHandler(stdout_log_handler)
    log.addHandler(file_log_handler)

    return log

log = get_log(PROCESS_NAME=PROCESS_NAME)



# NOTE: these functions will not work if `initialize_log` is not called first
def log_add_row(row) -> None:
    transaction_unique_id = row['transaction_unique_id']
    log_message = f'add row: transaction_unique_id={transaction_unique_id}'
    log.debug(log_message)


def log_change_row(row) -> None:
    transaction_unique_id = row['transaction_unique_id']
    log_message = f'change row: transaction_unique_id={transaction_unique_id}'
    log.debug(log_message)


def log_delete_row(row) -> None:
    transaction_unique_id = row['transaction_unique_id']
    log_message = f'delete row: transaction_unique_id={transaction_unique_id}'
    log.debug(log_message)



def add_row(
    session: Session,
    row,
    file_timestamp: datetime,
    input_file_statistics: InputFileStatistics,
):
    log_add_row(row)

    ignore_row_count_insert = None
    database_insert_row_count = None

    # this logic should only be required for the first month because the database
    # was initialized using pp-complete.txt which includes the monthly change
    # from pp-monthly-update.txt

    # NOTE: keep the logic, it guards against unexpected behaviour

    if check_row_exists_by_all_column_values(session, row):
        transaction_unique_id = row['transaction_unique_id']
        log.warning(f'row operation: ADD. (transaction_unique_id={transaction_unique_id}) row exists with expected column values, ignoring')

        ignore_row_count_insert = 1
        input_file_statistics.input_file_row_count_marked_as_add_but_already_identical_and_ignored += 1

    elif check_row_exists_by_tuid(session, row):
        transaction_unique_id = row['transaction_unique_id']
        log.warning(f'row operation: ADD. (transaction_unique_id={transaction_unique_id}) row exists but column values differ, changing')

        db_change_row(session, row, file_timestamp=file_timestamp)

        ignore_row_count_insert = 1
        input_file_statistics.input_file_row_count_marked_as_add_but_changed += 1

    elif check_row_exists_by_tuid_deleted(session, row):
        transaction_unique_id = row['transaction_unique_id']
        log.warning(f'row operation: ADD. (transaction_unique_id={transaction_unique_id}) row exists but marked as deleted, ignoring')

        ignore_row_count_insert = 1
        input_file_statistics.input_file_row_count_marked_as_add_but_deleted_and_ignored += 1

    # end of additional logic

    # additional logic to check for the TUID
    # if check_row_exists_by_tuid(session, row['transaction_unique_id']):
    #     log_add_change(row)

    #     if not change_row(session, row, datetime_now):
    #         transaction_unique_id = row['transaction_unique_id']
    #         log_message = f'change row: row with transaction_unique_id={transaction_unique_id} already exists'
    #         log.error(log_message)
    #         raise RuntimeError(log_message)
    #     else:
    #         database_change_row_count += 1
    else:
        transaction_unique_id = row['transaction_unique_id']
        log.debug(f'row operation: ADD. (transaction_unique_id={transaction_unique_id})')

        db_add_row(session, row, file_timestamp)

        database_insert_row_count = 1
        input_file_statistics.input_file_row_count_marked_as_add_and_added += 1

    d = {}
    if ignore_row_count_insert is not None:
        d['ignore_row_count_insert'] = ignore_row_count_insert
    if database_insert_row_count is not None:
        d['database_insert_row_count'] = database_insert_row_count
    return d


def change_row(
    session: Session,
    row,
    file_timestamp: datetime,
    input_file_statistics: InputFileStatistics,
):
    log_change_row(row)

    ignore_row_count_change = None
    database_change_row_count = None
    database_insert_row_count = None

    # this logic should only be required for the first month because the database
    # was initialized using pp-complete.txt which includes the monthly change
    # from pp-monthly-update.txt

    if check_row_exists_by_all_column_values(session, row):
        transaction_unique_id = row['transaction_unique_id']
        log.warning(f'row operation: CHANGE. (transaction_unique_id={transaction_unique_id}) row exists with identical column values, ignoring')

        ignore_row_count_change = 1
        input_file_statistics.input_file_row_count_marked_as_change_but_already_identical_and_ignored += 1

    elif check_row_exists_by_tuid(session, row):
        transaction_unique_id = row['transaction_unique_id']
        log.debug(f'row operation: CHANGE. (transaction_unique_id={transaction_unique_id})')

        db_change_row(session, row, file_timestamp)

        database_change_row_count = 1
        input_file_statistics.input_file_row_count_marked_as_change_and_changed += 1

    elif check_row_exists_by_tuid_deleted(session, row):
        transaction_unique_id = row['transaction_unique_id']
        log.warning(f'row operation: CHANGE. (transaction_unique_id={transaction_unique_id}) row exists but marked as deleted, ignoring')

        ignore_row_count_change = 1
        input_file_statistics.input_file_row_count_marked_as_change_but_deleted_and_ignored += 1

    else:
        transaction_unique_id = row['transaction_unique_id']
        log.warning(f'row operation: CHANGE. (transaction_unique_id={transaction_unique_id}) row is missing, adding row')

        db_add_row(session, row, file_timestamp)

        database_insert_row_count = 1
        input_file_statistics.input_file_row_count_marked_as_change_but_missing_and_added += 1

    d = {}
    if ignore_row_count_change is not None:
        d['ignore_row_count_change'] = ignore_row_count_change
    if database_change_row_count is not None:
        d['database_change_row_count'] = database_change_row_count
    if database_insert_row_count is not None:
        d['database_insert_row_count'] = database_insert_row_count
    return d

    # end of additional logic

    # if not change_row(session, row, datetime_now):
    #     transaction_unique_id = row['transaction_unique_id']
    #     log_message = f'change row: row with transaction_unique_id={transaction_unique_id} already exists'
    #     log.error(log_message)
    #     raise RuntimeError(log_message)
    # else:
    #     database_change_row_count += 1


def delete_row(
    session: Session,
    row,
    file_timestamp: datetime,
    input_file_statistics: InputFileStatistics,
):
    log_delete_row(row)

    database_delete_row_count = None
    ignore_row_count_delete = None

    # this logic should only be required for the first month because the database
    # was initialized using pp-complete.txt which includes the monthly change
    # from pp-monthly-update.txt


    if check_row_exists_by_all_column_values(session, row):
        transaction_unique_id = row['transaction_unique_id']
        log.debug(f'row operation: DELETE. (transaction_unique_id={transaction_unique_id})')

        db_delete_row(session, row, file_timestamp=file_timestamp)

        database_delete_row_count = 1
        input_file_statistics.input_file_row_count_marked_as_delete_and_deleted += 1

    elif check_row_exists_by_tuid(session, row):
        transaction_unique_id = row['transaction_unique_id']
        log.warning(f'row operation: DELETE. (transaction_unique_id={transaction_unique_id}) row exists but column values differ, deleting')

        db_change_row(session, row, file_timestamp=file_timestamp)
        db_delete_row(session, row, file_timestamp=file_timestamp)

        database_delete_row_count = 1
        input_file_statistics.input_file_row_count_marked_as_delete_but_not_identical_and_changed_and_deleted += 1

    elif check_row_exists_by_tuid_deleted(session, row):
        transaction_unique_id = row['transaction_unique_id']
        log.warning(f'row operation: DELETE. (transaction_unique_id={transaction_unique_id}) row exists but marked as deleted, ignoring')

        ignore_row_count_delete = 1
        input_file_statistics.input_file_row_count_marked_as_delete_but_deleted_and_ignored += 1

    else:
        transaction_unique_id = row['transaction_unique_id']
        log.warning(f'row operation: DELETE. (transaction_unique_id={transaction_unique_id}) row does not exist, ignoring')

        ignore_row_count_delete = 1
        input_file_statistics.input_file_row_count_marked_as_delete_but_missing_and_ignored += 1

    d = {}
    if database_delete_row_count is not None:
        d['database_delete_row_count'] = database_delete_row_count
    if ignore_row_count_delete is not None:
        d['ignore_row_count_delete'] = ignore_row_count_delete
    return d

    # if not check_row_exists_by_tuid(session, row):
    #     ignore_row_count_delete += 1
    #     #log_delete_row_does_not_exist(producer, row)
    #     continue

    # # end of additional logic

    # if not db_delete_row_if_tuid_exists(session, row, datetime_now):
    #     transaction_unique_id = row['transaction_unique_id']
    #     log_message = f'missing row for transaction_unique_id={transaction_unique_id}'
    #     log.error(log_message)
    #     raise RuntimeError(log_message)
    # else:
    #     database_delete_row_count += 1







class PathIsNotAFileError(OSError):

    def __init__(self, path, message):
        self.path = path
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return f'{self.message}, path={self.path}'


def main():

    log.info(f'process {PROCESS_NAME} started up')

    kafka_bootstrap_servers = os.environ['KAFKA_BOOTSTRAP_SERVERS']
    log.info(f'Kafka bootstrap server address: {kafka_bootstrap_servers}')

    log.info(
        f'creating Kafka consumer with '
        f'bootstrap_servers={kafka_bootstrap_servers} '
        f'client_id={CLIENT_ID}, '
        f'group_id={GROUP_ID}'
    )
    consumer = create_consumer(
        bootstrap_servers=kafka_bootstrap_servers,
        client_id=CLIENT_ID,
        group_id=GROUP_ID,
    )

    log.info(
        f'creating Kafka producer with '
        f'bootstrap_servers={kafka_bootstrap_servers} '
        f'client_id={CLIENT_ID}, '
    )
    producer = create_producer(
        bootstrap_servers=kafka_bootstrap_servers,
        client_id=CLIENT_ID,
    )

    log.info(f'run main process')
    run_process(consumer, producer)
    log.info(f'main process exit')


def run_process(
    consumer: Consumer,
    producer: Producer,
) -> None:

    log.info(f'consumer subscribing to topic {topic_name_land_registry_download_monthly_update_data_decision_notification}')
    consumer.subscribe([topic_name_land_registry_download_monthly_update_data_decision_notification])

    consumer_poll_timeout = 10.0
    log.debug(f'consumer poll timeout: {consumer_poll_timeout}')

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
            log.error(f'error from kafka: {message.value().decode()}')
            raise RuntimeError(f'{message.value().decode()}')
            # TODO: add expliclt handling for different types of error
            # then remove exception?
        else:
            dto = jsons.loads(
                message.value().decode(),
                MonthlyUpdateDataDecisionCompleteNotificationDTO,
            )

            try:
                notification_source = dto.notification_source

                if (
                    notification_source == OLD_PROCESS_NAME_LAND_REGISTRY_MONTHLY_UPDATE_DATA_DECISION or
                    notification_source == PROCESS_NAME_LAND_REGISTRY_MONTHLY_UPDATE_DATA_DECISION
                ):
                    notification_type = dto.notification_type

                    if notification_type == DAILY_DOWNLOAD_MONTHLY_UPDATE_DATA_DECISION_COMPLETE:

                        thread_handle = threading.Thread(target=consumer_poll_loop, args=(consumer,))
                        thread_handle.start()

                        filename = dto.filename
                        sha256sum = dto.sha256sum
                        decision = dto.data_decision
                        log.info(f'processing message: filename={filename}, sha256sum={sha256sum}, decision={decision}')
                        assert len(sha256sum) > 0

                        if decision == 'ignored':
                            log_message = f'ignoring file {filename}'
                            log.info(log_message)

                            notify_ignored(
                                producer,
                                filename,
                                sha256sum=dto.sha256sum,
                                data_decision=dto.data_decision,
                                data_decision_dto=dto,
                            )

                        elif decision == 'processed':
                            log_message = f'processing file {filename}'
                            log.info(log_message)

                            log.info(f'run_process: database update: filename={filename}')

                            return_value = update_database(filename, file_timestamp=dto.timestamp_download)

                            if return_value is None:
                                pass
                            else:
                                (
                                    file_row_count,
                                    file_row_count_insert,
                                    file_row_count_change,
                                    file_row_count_delete,
                                    database_row_count_before,
                                    database_row_count_after,
                                    input_file_statistics,
                                ) = return_value

                                log.info(f'run_process: database update: filename={filename} [completed]')

                                notify_processed(
                                    producer,
                                    filename,
                                    sha256sum=dto.sha256sum,
                                    data_decision=dto.data_decision,
                                    file_row_count=file_row_count,
                                    file_row_count_insert=file_row_count_insert,
                                    file_row_count_change=file_row_count_change,
                                    file_row_count_delete=file_row_count_delete,
                                    database_row_count_before=database_row_count_before,
                                    database_row_count_after=database_row_count_after,
                                )

                        event_thead_terminate.set()
                        thread_handle.join()
                        log.debug(f'consumer commit offset={message.offset()}')
                        consumer.commit()
                        event_thead_terminate.clear()

                    else:
                        log.error(f'unknown notification type: {notification_type}')
                else:
                    log.error(f'unknown notification source: {notification_source}')

            except Exception as exception:
                log.error(f'{exception}')

    logging.info(f'consumer unsubscribe')
    consumer.unsubscribe()
    consumer.close()


def consumer_poll_loop(consumer: Consumer) -> None:

    topic_partition_assignment = consumer.assignment()
    consumer.pause(topic_partition_assignment)

    while True:
        consumer_short_poll_duration = 1.0
        message = consumer.poll(consumer_short_poll_duration)

        if message is not None:
            raise RuntimeError(f'consumer abort')

        if event_thead_terminate.is_set():
            break

    consumer.resume(topic_partition_assignment)


def notify_ignored(
    producer: Producer,
    filename: str,
    sha256sum: str,
    data_decision: str,
    data_decision_dto: MonthlyUpdateDataDecisionCompleteNotificationDTO,
) -> None:
    now = datetime.now(timezone.utc)

    dto = MonthlyUpdateDatabaseUpdateCompleteNotificationDTO(
        notification_source=PROCESS_NAME,
        notification_type=DAILY_DOWNLOAD_MONTHLY_UPDATE_DATABASE_UPDATE_COMPLETE,
        timestamp=now,
        filename=filename,
        sha256sum=sha256sum,
        data_decision=data_decision,
        file_row_count=None,
        file_row_count_insert=None,
        file_row_count_change=None,
        file_row_count_delete=None,
        database_row_count_before=None,
        database_row_count_after=None,
        timestamp_cron_trigger=data_decision_dto.timestamp_cron_trigger,
        timestamp_download=data_decision_dto.timestamp_download,
        timestamp_shasum=data_decision_dto.timestamp_shasum,
        timestamp_data_decision=data_decision_dto.timestamp_data_decision,
        timestamp_database_upload=None,
    )

    document_json_str = jsons.dumps(dto)

    producer.produce(
        topic=topic_name_land_registry_download_monthly_update_database_updater_notification,
        key=f'no_key',
        value=document_json_str,
    )

    producer.flush()


def notify_processed(
    producer: Producer,
    filename: str,
    sha256sum: str,
    data_decision: str,
    file_row_count: int,
    file_row_count_insert: int,
    file_row_count_change: int,
    file_row_count_delete: int,
    database_row_count_before: int,
    database_row_count_after: int,
    data_decision_dto: MonthlyUpdateDataDecisionCompleteNotificationDTO,
) -> None:
    now = datetime.now(timezone.utc)

    document = MonthlyUpdateDatabaseUpdateCompleteNotificationDTO(
        notification_source=PROCESS_NAME,
        notification_type='daily_download_monthly_update_database_update_complete',
        timestamp=now,
        filename=filename,
        sha256sum=sha256sum,
        data_decision=data_decision,
        file_row_count=file_row_count,
        file_row_count_insert=file_row_count_insert,
        file_row_count_change=file_row_count_change,
        file_row_count_delete=file_row_count_delete,
        database_row_count_before=database_row_count_before,
        database_row_count_after=database_row_count_after,
        timestamp_cron_trigger=data_decision_dto.timestamp_cron_trigger,
        timestamp_download=data_decision_dto.timestamp_download,
        timestamp_shasum=data_decision_dto.timestamp_shasum,
        timestamp_data_decision=data_decision_dto.timestamp_data_decision,
        timestamp_database_upload=now,
    )

    document_json_str = jsons.dumps(document)

    producer.produce(
        topic=topic_name_land_registry_download_monthly_update_database_updater_notification,
        key=f'no_key',
        value=document_json_str,
    )

    producer.flush()


def format_dataframe(df: pandas.DataFrame) -> pandas.DataFrame:

    df.columns = [
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
        'record_status',
    ]

    df.index.names = ['price_paid_data_id']

    df = df.astype(
        {
            'transaction_unique_id': str,
            'price': int,
            'postcode': str,
            'property_type': str,
            'new_tag': str,
            'lease': str,
            'primary_address_object_name': str,
            'secondary_address_object_name': str,
            'street': str,
            'locality': str,
            'town_city': str,
            'district': str,
            'county': str,
            'ppd_cat': str,
            'record_status': str,
        }
    )

    df['transaction_date'] = pandas.to_datetime(
        arg=df['transaction_date'],
        utc=True,
        format='%Y-%m-%d %H:%M',
    )

    df.fillna('', inplace=True)

    return df


def check_row_exists_by_all_column_values(session: Session, row) -> bool:
    existing_row = (
        session
        .query(PricePaidData)
        .filter_by(transaction_unique_id=row['transaction_unique_id'])
        .filter_by(price=row['price'])
        .filter_by(transaction_date=row['transaction_date'])
        .filter_by(postcode=row['postcode'])
        .filter_by(property_type=row['property_type'])
        .filter_by(new_tag=row['new_tag'])
        .filter_by(lease=row['lease'])
        .filter_by(primary_address_object_name=row['primary_address_object_name'])
        .filter_by(secondary_address_object_name=row['secondary_address_object_name'])
        .filter_by(street=row['street'])
        .filter_by(locality=row['locality'])
        .filter_by(town_city=row['town_city'])
        .filter_by(district=row['district'])
        .filter_by(county=row['county'])
        .filter_by(ppd_cat=row['ppd_cat'])
        .filter_by(is_deleted=False) # TODO: remove?
        .one_or_none()
    )
    return existing_row is not None


def check_row_exists_by_tuid(session: Session, row) -> bool:
    existing_row = (
        session
        .query(PricePaidData)
        .filter_by(transaction_unique_id=row['transaction_unique_id'])
        .filter_by(is_deleted=False)
        .one_or_none()
    )
    return existing_row is not None


def check_row_exists_by_tuid_deleted(session: Session, row) -> bool:
    existing_row = (
        session
        .query(PricePaidData)
        .filter_by(transaction_unique_id=row['transaction_unique_id'])
        .filter_by(is_deleted=True)
        .one_or_none()
    )
    return existing_row is not None


def db_add_row(
    session: Session,
    row,
    file_timestamp: datetime,
) -> None:
    # TODO: add a check for transaction_unique_id existing
    new_row = PricePaidData(
        transaction_unique_id=row['transaction_unique_id'],
        price=row['price'],
        transaction_date=row['transaction_date'],
        postcode=row['postcode'],
        property_type=row['property_type'],
        new_tag=row['new_tag'],
        lease=row['lease'],
        primary_address_object_name=row['primary_address_object_name'],
        secondary_address_object_name=row['secondary_address_object_name'],
        street=row['street'],
        locality=row['locality'],
        town_city=row['town_city'],
        district=row['district'],
        county=row['county'],
        ppd_cat=row['ppd_cat'],
        record_status=row['record_status'],
        is_deleted=False,
        created_datetime=file_timestamp,
        updated_datetime=None,
        deleted_datetime=None,
        created_datetime_original=None,
    )
    session.add(new_row)
    session.commit()


def db_change_row(
    session: Session,
    row,
    file_timestamp: datetime,
) -> None:
    existing_row = (
        session
        .query(PricePaidData)
        .filter_by(transaction_unique_id=row['transaction_unique_id'])
        .filter_by(is_deleted=False)
        .one()
    )
    existing_row.price = row['price']
    existing_row.transaction_date = row['transaction_date']
    existing_row.postcode = row['postcode']
    existing_row.property_type = row['property_type']
    existing_row.new_tag = row['new_tag']
    existing_row.lease = row['lease']
    existing_row.primary_address_object_name = row['primary_address_object_name']
    existing_row.secondary_address_object_name = row['secondary_address_object_name']
    existing_row.street = row['street']
    existing_row.locality = row['locality']
    existing_row.town_city = row['town_city']
    existing_row.district = row['district']
    existing_row.county = row['county']
    existing_row.ppd_cat = row['ppd_cat']
    existing_row.record_status = row['record_status']
    existing_row.updated_datetime = file_timestamp
    session.commit()


def db_delete_row(
    session: Session,
    row,
    file_timestamp: datetime,
) -> None:
    existing_row = (
        session
        .query(PricePaidData)
        .filter_by(transaction_unique_id=row['transaction_unique_id'])
        .filter_by(is_deleted=False)
        .one()
    )
    existing_row.is_deleted = True
    existing_row.deleted_datetime = file_timestamp
    session.commit()


# def db_delete_row_if_tuid_exists(session: Session, row, datetime_now: datetime) -> bool:

#     existing_row = (
#         session
#         .query(PricePaidData)
#         .filter_by(transaction_unique_id=row['transaction_unique_id'])
#         .one_or_none()
#     )

#     if existing_row is None:
#         return False
#     else:
#         existing_row.is_deleted = True
#         existing_row.deleted_datetime = datetime_now

#         session.commit()
#         return True


def get_file_path(filename: str) -> str:
    data_directory = '/data-land-registry/pp-monthly-update'
    return f'{data_directory}/{filename}'


def update_database(
    filename: str,
    file_timestamp: datetime,
) -> tuple:
    postgres_address = os.environ['POSTGRES_ADDRESS']
    postgres_user = os.environ['POSTGRES_USER']
    postgres_password = os.environ['POSTGRES_PASSWORD']
    postgres_database = os.environ['POSTGRES_DATABASE']
    postgres_connection_string = f'postgresql://{postgres_user}:{postgres_password}@{postgres_address}/{postgres_database}'
    # TODO: move

    #url = 'postgresql://user:password@host/postgres'
    engine_postgres = create_engine(postgres_connection_string)

    with Session(engine_postgres) as session:

        # check to see if file has already been uploaded
        price_paid_data_monthly_update_file_log_row = (
            session
            .query(PricePaidDataMonthlyUpdateFileLog)
            .filter_by(filename=filename)
            .one_or_none()
        )

        # expect to see a pre-existing row
        if price_paid_data_monthly_update_file_log_row is None:
            log_message = f'missing row, filename {filename}'
            log.error(log_message)
            raise RuntimeError(log_message)
        else:
            print(price_paid_data_monthly_update_file_log_row)
            uploaded_datetime = price_paid_data_monthly_update_file_log_row.uploaded_datetime

            if uploaded_datetime is not None:
                log.warning(f'ignoring file {filename}, already marked with uploaded datetime {uploaded_datetime}')
                return None
            else:
                # load data from file
                file_path = get_file_path(filename)
                log.info(f'update_database: filename={filename}')

                if not os.path.exists(file_path):
                    log_message = f'{file_path} does not exist'
                    log.error(log_message)
                    raise FileNotFoundError(log_message)

                if not os.path.isfile(file_path):
                    log_message = f'{file_path} is not a file'
                    log.error(log_message)
                    raise PathIsNotAFileError(file_path, log_message)

                df = pandas.read_csv(
                    file_path,
                    sep=',',
                    quotechar='"',
                    dtype=str,
                    header=None,
                    keep_default_na=False,
                    # coerce_float=False,
                    # parse_dates=False,
                )

                df = format_dataframe(df)

                datetime_now = datetime.now(timezone.utc)

                # process data
                input_file_statistics = InputFileStatistics(
                    input_file_row_count=len(df),
                    input_file_row_count_marked_as_add=0,
                    input_file_row_count_marked_as_change=0,
                    input_file_row_count_marked_as_delete=0,
                    input_file_row_count_marked_as_add_and_added=0,
                    input_file_row_count_marked_as_add_but_already_identical_and_ignored=0,
                    input_file_row_count_marked_as_add_but_changed=0,
                    input_file_row_count_marked_as_add_but_deleted_and_ignored=0,
                    input_file_row_count_marked_as_change_and_changed=0,
                    input_file_row_count_marked_as_change_but_already_identical_and_ignored=0,
                    input_file_row_count_marked_as_change_but_missing_and_added=0,
                    input_file_row_count_marked_as_change_but_deleted_and_ignored=0,
                    input_file_row_count_marked_as_delete_and_deleted=0,
                    input_file_row_count_marked_as_delete_but_not_identical_and_changed_and_deleted=0,
                    input_file_row_count_marked_as_delete_but_missing_and_ignored=0,
                    input_file_row_count_marked_as_delete_but_deleted_and_ignored=0,
                )

                # number of rows in database table, before and after update
                row_count_before = None
                row_count_after = None

                # number of rows inserted, changed and deleted. the counts correspond to
                # the input data file. each row in the input causes either an insert,
                # change or delete operation, or the row is ignored.
                # these counters count those operations.
                # the sum of these values should be equal to the number of row in the input
                # file, however this may not be the case if some of those rows are ignored.
                database_insert_row_count = 0
                database_change_row_count = 0
                database_delete_row_count = 0

                # these variables count the number of rows which are ignored, for each
                # expected operation. For example, `ignore_row_count_insert` counts the
                # number of rows which are ignored where those rows were expected to be
                # inserted.
                ignore_row_count_insert = 0
                ignore_row_count_change = 0
                ignore_row_count_delete = 0

                row_count_before = session.query(PricePaidData).count()
                log.info(f'row_count_before={row_count_before}')

                log.info(f'number of rows in DataFrame to process: {len(df)}')

                for index, price_paid_data_monthly_update_row in df.iterrows():
                    record_status = price_paid_data_monthly_update_row['record_status']
                    log.debug(f'record_status={record_status}')

                    if record_status == 'A':
                        input_file_statistics.input_file_row_count_marked_as_add += 1
                        d = add_row(
                            session,
                            price_paid_data_monthly_update_row,
                            file_timestamp=file_timestamp,
                            input_file_statistics=input_file_statistics,
                        )
                        if 'database_insert_row_count' in d: database_insert_row_count += d['database_insert_row_count']
                        if 'database_change_row_count' in d: database_change_row_count += d['database_change_row_count']
                        if 'database_delete_row_count' in d: database_delete_row_count += d['database_delete_row_count']
                        if 'ignore_row_count_insert' in d: ignore_row_count_insert += d['ignore_row_count_insert']
                        if 'ignore_row_count_change' in d: ignore_row_count_change += d['ignore_row_count_change']
                        if 'ignore_row_count_delete' in d: ignore_row_count_delete += d['ignore_row_count_delete']

                    elif record_status == 'C':
                        input_file_statistics.input_file_row_count_marked_as_change += 1
                        d = change_row(
                            session,
                            price_paid_data_monthly_update_row,
                            file_timestamp=file_timestamp,
                            input_file_statistics=input_file_statistics,
                        )
                        if 'database_insert_row_count' in d: database_insert_row_count += d['database_insert_row_count']
                        if 'database_change_row_count' in d: database_change_row_count += d['database_change_row_count']
                        if 'database_delete_row_count' in d: database_delete_row_count += d['database_delete_row_count']
                        if 'ignore_row_count_insert' in d: ignore_row_count_insert += d['ignore_row_count_insert']
                        if 'ignore_row_count_change' in d: ignore_row_count_change += d['ignore_row_count_change']
                        if 'ignore_row_count_delete' in d: ignore_row_count_delete += d['ignore_row_count_delete']

                    elif record_status == 'D':
                        input_file_statistics.input_file_row_count_marked_as_delete += 1
                        d = delete_row(
                            session,
                            price_paid_data_monthly_update_row,
                            file_timestamp=file_timestamp,
                            input_file_statistics=input_file_statistics,
                        )
                        if 'database_insert_row_count' in d: database_insert_row_count += d['database_insert_row_count']
                        if 'database_change_row_count' in d: database_change_row_count += d['database_change_row_count']
                        if 'database_delete_row_count' in d: database_delete_row_count += d['database_delete_row_count']
                        if 'ignore_row_count_insert' in d: ignore_row_count_insert += d['ignore_row_count_insert']
                        if 'ignore_row_count_change' in d: ignore_row_count_change += d['ignore_row_count_change']
                        if 'ignore_row_count_delete' in d: ignore_row_count_delete += d['ignore_row_count_delete']

                    else:
                        # input_file_statistics <-- not supported here
                        log_message = f'unrecognized record_status={record_status}'
                        log.error(log_message)
                        raise RuntimeError(log_message)

                log_message = (
                    f'ignored row count: '
                    f'insert={ignore_row_count_insert} '
                    f'change={ignore_row_count_change} '
                    f'delete={ignore_row_count_delete}'
                )
                log.warning(log_message)

                log_message = (
                    f'insert count: {database_insert_row_count}, '
                    f'change count: {database_change_row_count}, '
                    f'delete count: {database_delete_row_count}'
                )
                log.info(log_message)

                row_count_after = session.query(PricePaidData).count()
                log.info(f'row_count_after={row_count_after}')

                log_message = (
                    f'database row count before: {row_count_before}, '
                    f'database row count after: {row_count_after}'
                )
                log.info(log_message)

                price_paid_data_log_new_row = PricePaidDataLog(
                    log_timestamp=datetime_now,
                    query_row_count_total=len(df),
                    query_row_count_inserted=database_insert_row_count,
                    query_row_count_updated=database_change_row_count,
                    query_row_count_deleted=database_delete_row_count,
                    row_count_before=row_count_before,
                    row_count_after=row_count_after,
                )
                session.add(price_paid_data_log_new_row)
                session.commit()

                log_message = f'updating row with filename {filename}, file timestamp {file_timestamp}, to uploaded_datetime={datetime_now}'
                log.info(log_message)

                price_paid_data_monthly_update_file_log_row.uploaded_datetime = datetime_now
                session.commit()

                log.info(f'updating database table {PricePaidDataMonthlyUpdateDatabaseUpdaterOperationLog.__tablename__}')

                total_insert = (
                    input_file_statistics.input_file_row_count_marked_as_add_and_added +
                    input_file_statistics.input_file_row_count_marked_as_change_but_missing_and_added
                )
                total_update = (
                    input_file_statistics.input_file_row_count_marked_as_add_but_changed +
                    input_file_statistics.input_file_row_count_marked_as_change_and_changed
                )
                total_delete = (
                    input_file_statistics.input_file_row_count_marked_as_delete_and_deleted +
                    input_file_statistics.input_file_row_count_marked_as_delete_but_not_identical_and_changed_and_deleted
                )
                total_ignore = (
                    input_file_statistics.input_file_row_count_marked_as_add_but_already_identical_and_ignored +
                    input_file_statistics.input_file_row_count_marked_as_change_but_already_identical_and_ignored +
                    input_file_statistics.input_file_row_count_marked_as_delete_but_missing_and_ignored +
                    input_file_statistics.input_file_row_count_marked_as_add_but_deleted_and_ignored +
                    input_file_statistics.input_file_row_count_marked_as_change_but_deleted_and_ignored +
                    input_file_statistics.input_file_row_count_marked_as_delete_but_deleted_and_ignored
                )

                price_paid_data_monthly_update_database_updater_operation_log_row = (
                    PricePaidDataMonthlyUpdateDatabaseUpdaterOperationLog(
                        filename=filename,
                        processed_datetime=datetime_now,
                        input_file_row_count=len(df),
                        input_file_row_count_insert=input_file_statistics.input_file_row_count_marked_as_add,
                        input_file_row_count_update=input_file_statistics.input_file_row_count_marked_as_change,
                        input_file_row_count_delete=input_file_statistics.input_file_row_count_marked_as_delete,
                        operation_count_insert=total_insert,
                        operation_count_update=total_update,
                        operation_count_delete=total_delete,
                        operation_count_ignored=total_ignore,
                        operation_count_insert_insert=input_file_statistics.input_file_row_count_marked_as_add_and_added,
                        operation_count_insert_update=input_file_statistics.input_file_row_count_marked_as_add_but_changed,
                        operation_count_insert_ignore=(
                            input_file_statistics.input_file_row_count_marked_as_add_but_already_identical_and_ignored +
                            input_file_statistics.input_file_row_count_marked_as_add_but_deleted_and_ignored
                        ),
                        operation_count_update_update=input_file_statistics.input_file_row_count_marked_as_change_and_changed,
                        operation_count_update_insert=input_file_statistics.input_file_row_count_marked_as_change_but_missing_and_added,
                        operation_count_update_ignore=(
                            input_file_statistics.input_file_row_count_marked_as_change_but_already_identical_and_ignored +
                            input_file_statistics.input_file_row_count_marked_as_change_but_deleted_and_ignored
                        ),
                        operation_count_delete_delete=input_file_statistics.input_file_row_count_marked_as_delete_and_deleted,
                        operation_count_delete_change_delete=input_file_statistics.input_file_row_count_marked_as_delete_but_not_identical_and_changed_and_deleted,
                        operation_count_delete_ignore=(
                            input_file_statistics.input_file_row_count_marked_as_delete_but_missing_and_ignored +
                            input_file_statistics.input_file_row_count_marked_as_delete_but_deleted_and_ignored
                        ),
                    )
                )
                session.add(price_paid_data_monthly_update_database_updater_operation_log_row)
                session.commit()

                return (
                    len(df),
                    database_insert_row_count,
                    database_change_row_count,
                    database_delete_row_count,
                    row_count_before,
                    row_count_after,
                    input_file_statistics,
                )




# def log_add_change(row) -> None:

#     transaction_unique_id = row['transaction_unique_id']
#     log_message = f'add row: (change row, tuid exists) transaction_unique_id={transaction_unique_id}'
#     log.debug(log_message)




# def log_delete_row_does_not_exist(row) -> None:

#     transaction_unique_id = row['transaction_unique_id']
#     log_message = f'delete row: row with transaction_unique_id={transaction_unique_id} does not exist'
#     log.warning(log_message)




exit_flag = False

def ctrl_c_signal_handler(signal, frame):
    # TODO: ideally want to add this back in
    #print(f'wait for exit...')
    global exit_flag
    exit_flag = True

def sigterm_signal_handler(signal, frame):
    log.info(f'SIGTERM')
    global exit_flag
    exit_flag = True


if __name__ == '__main__':
    signal.signal(signal.SIGINT, ctrl_c_signal_handler)
    signal.signal(signal.SIGTERM, sigterm_signal_handler)
    main()


