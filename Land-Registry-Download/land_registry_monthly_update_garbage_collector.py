#!/usr/bin/env python3

import jsons
from datetime import datetime
from datetime import timezone
from datetime import timedelta

import os
import signal
import hashlib
import threading

from lib_land_registry_download.lib_kafka import create_consumer
from lib_land_registry_download.lib_kafka import create_producer

from confluent_kafka import Consumer
from confluent_kafka import Producer

from lib_land_registry_download.lib_topic_name import topic_name_land_registry_download_monthly_update_database_updater_notification
from lib_land_registry_download.lib_topic_name import topic_name_land_registry_download_monthly_update_garbage_collector_notification

from lib_land_registry_download.lib_dto import MonthlyUpdateDatabaseUpdateCompleteNotificationDTO
from lib_land_registry_download.lib_dto import MonthlyUpdateGarbageCollectorCompleteNotificationDTO

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from lib_land_registry_download.lib_db import PricePaidDataMonthlyUpdateFileLog

import logging
import sys

from lib_land_registry_download.lib_constants.process_name import PROCESS_NAME_LAND_REGISTRY_MONTHLY_UPDATE_GARBAGE_COLLECTOR as PROCESS_NAME
from lib_land_registry_download.lib_constants.process_name import PROCESS_NAME_LAND_REGISTRY_MONTHLY_UPDATE_GARBAGE_COLLECTOR as CLIENT_ID
from lib_land_registry_download.lib_constants.process_name import PROCESS_NAME_LAND_REGISTRY_MONTHLY_UPDATE_GARBAGE_COLLECTOR as GROUP_ID
from lib_land_registry_download.lib_constants.process_name import PROCESS_NAME_LAND_REGISTRY_MONTHLY_UPDATE_DATABASE_UPDATER
from lib_land_registry_download.lib_constants.process_name import OLD_PROCESS_NAME_LAND_REGISTRY_MONTHLY_UPDATE_DATABASE_UPDATER
from lib_land_registry_download.lib_constants.notification_type import DAILY_DOWNLOAD_MONTHLY_UPDATE_DATABASE_UPDATE_COMPLETE
from lib_land_registry_download.lib_constants.notification_type import DAILY_DOWNLOAD_MONTHLY_UPDATE_GARBAGE_COLLECTION_COMPLETE


class PathIsNotAFileError(OSError):

    def __init__(self, path, message):
        self.path = path
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return f'{self.message}, path={self.path}'


event_thead_terminate = threading.Event()


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
file_log_handler.setLevel(logging.DEBUG)
file_log_handler.setFormatter(file_log_formatter)

log.setLevel(logging.DEBUG)
log.addHandler(stdout_log_handler)
log.addHandler(file_log_handler)


def main():
    log.info(f'{PROCESS_NAME} start')
    kafka_bootstrap_servers = os.environ['KAFKA_BOOTSTRAP_SERVERS']

    consumer = create_consumer(
        bootstrap_servers=kafka_bootstrap_servers,
        client_id=CLIENT_ID,
        group_id=GROUP_ID,
    )

    producer = create_producer(
        bootstrap_servers=kafka_bootstrap_servers,
        client_id=CLIENT_ID,
    )

    run_process(consumer, producer)


def run_process(
    consumer: Consumer,
    producer: Producer,
) -> None:

    consumer.subscribe([topic_name_land_registry_download_monthly_update_database_updater_notification])
    log.info(f'consumer subscribing to topic {topic_name_land_registry_download_monthly_update_database_updater_notification}')

    consumer_poll_timeout = 10.0

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
            log.error(log_message)
            raise RuntimeError(f'{message.value().decode()}')
        else:
            dto = jsons.loads(
                message.value().decode(),
                MonthlyUpdateDatabaseUpdateCompleteNotificationDTO,
            )

            try:
                notification_source = dto.notification_source
                log.debug(f'notification source: {notification_source}')

                if (
                    notification_source == OLD_PROCESS_NAME_LAND_REGISTRY_MONTHLY_UPDATE_DATABASE_UPDATER or
                    notification_source == PROCESS_NAME_LAND_REGISTRY_MONTHLY_UPDATE_DATABASE_UPDATER
                ):
                    notification_type = dto.notification_type
                    log.debug(f'notification type: {notification_type}')

                    if notification_type == DAILY_DOWNLOAD_MONTHLY_UPDATE_DATABASE_UPDATE_COMPLETE:

                        thread_handle = threading.Thread(target=consumer_poll_loop, args=(consumer,))
                        thread_handle.start()

                        log.info(f'run_process: garbage collection')
                        try:
                            # new notification documents just trigger the garbage collection
                            # process for all old files
                            garbage_collect(producer=producer)

                        except FileNotFoundError as exception:
                            log.error(str(exception))
                        except PathIsNotAFileError as exception:
                            log.error(str(exception))

                        event_thead_terminate.set()
                        thread_handle.join()
                        consumer.commit()
                        event_thead_terminate.clear()

                    else:
                        raise RuntimeError(f'unknown notification type: {notification_type}')
                else:
                    raise RuntimeError(f'unknown notification source: {notification_source}')

            except Exception as exception:
                log_message = f'notification error: {exception}'
                log.error(log_message)

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


def get_file_path(filename: str) -> str:
    data_directory = '/data-land-registry/pp-monthly-update'
    return f'{data_directory}/{filename}'


def notify(
    producer: Producer,
    filename: str,
    dto: MonthlyUpdateDatabaseUpdateCompleteNotificationDTO,
) -> None:
    now = datetime.now(timezone.utc)

    gc_dto = MonthlyUpdateGarbageCollectorCompleteNotificationDTO(
        notification_source=PROCESS_NAME,
        notification_type=DAILY_DOWNLOAD_MONTHLY_UPDATE_GARBAGE_COLLECTION_COMPLETE,
        timestamp=now,
        filename=filename,
        timestamp_cron_trigger=dto.timestamp_cron_trigger,
        timestamp_download=dto.timestamp_download,
        timestamp_shasum=dto.timestamp_shasum,
        timestamp_data_decision=dto.timestamp_data_decision,
        timestamp_database_upload=dto.timestamp_database_upload,
        timestamp_garbage_collect=now,
    )

    document_json_str = jsons.dumps(gc_dto)

    producer.produce(
        topic=topic_name_land_registry_download_monthly_update_garbage_collector_notification,
        key=f'no_key',
        value=document_json_str,
    )

    producer.flush()


def garbage_collect(
    producer: Producer,
    dto: MonthlyUpdateDatabaseUpdateCompleteNotificationDTO,
) -> None:

    postgres_address = os.environ['POSTGRES_ADDRESS']
    postgres_user = os.environ['POSTGRES_USER']
    postgres_password = os.environ['POSTGRES_PASSWORD']
    postgres_database = os.environ['POSTGRES_DATABASE']
    postgres_connection_string = f'postgresql://{postgres_user}:{postgres_password}@{postgres_address}/{postgres_database}'
    # TODO: move

    now = datetime.now(timezone.utc)
    log.info(f'garbage_collect: now={now}')

    #url = 'postgresql://user:password@host/postgres'
    log.debug(f'opening database session to {postgres_connection_string}')
    engine_postgres = create_engine(postgres_connection_string)

    with Session(engine_postgres) as session:

        existing_rows = (
            session
            .query(PricePaidDataMonthlyUpdateFileLog)
            .filter_by(process_decision='ignored')
            .filter_by(deleted_datetime=None)
            .all()
        )
        log.debug(f'querying database table PricePaidDataMonthlyUpdateFileLog for rows with \'ignored\' process decision and no deleted datetime, {len(existing_rows)} match database query')

        for existing_row in existing_rows:
            filename = existing_row.filename
            log.info(f'existing row with filename {filename}')

            process_decision = existing_row.process_decision
            if process_decision == 'ignored':
                created_datetime = existing_row.created_datetime
                file_age: timedelta = now - created_datetime

                # this won't actually work, because the notification will
                # always arrive when the file is < 10 days old
                if file_age > timedelta(days=10):
                    existing_row.deleted_datetime = now
                    session.commit()

                    file_path = get_file_path(filename)
                    log.info(f'delete file: {filename}, age: {file_age}')
                    log.info(f'path: {file_path}')

                    if not os.path.exists(file_path):
                        log_message = f'{file_path} does not exist'
                        log.error(log_message)
                        raise FileNotFoundError(log_message)

                    if not os.path.isfile(file_path):
                        log_message = f'{file_path} is not a file'
                        log.error(log_message)
                        raise PathIsNotAFileError(file_path, log_message)

                    os.remove(file_path)
                    notify(producer, filename, dto)
                else:
                    log.info(f'ignore file: {filename}, age: {file_age}')
            else:
                log.info(f'ignoring file {filename} with process_decision={process_decision}')


exit_flag = False

def ctrl_c_signal_handler(signal, frame):
    log.info(f'CTRL^C wait for exit...')
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


