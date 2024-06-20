#!/usr/bin/env python3

import jsons
from datetime import datetime
from datetime import timezone

import os
import signal
import hashlib
import threading

from lib_land_registry_download.lib_kafka import create_consumer
from lib_land_registry_download.lib_kafka import create_producer

from confluent_kafka import Consumer
from confluent_kafka import Producer

from lib_land_registry_download.lib_topic_name import topic_name_land_registry_download_monthly_update_downloader_notification
from lib_land_registry_download.lib_topic_name import topic_name_land_registry_download_monthly_update_sha256_calculator_notification

from lib_land_registry_download.lib_dto import MonthlyUpdateDownloadCompleteNotificationDTO
from lib_land_registry_download.lib_dto import MonthlyUpdateSHA256CalculationCompleteNotificationDTO

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from lib_land_registry_download.lib_db import PricePaidDataMonthlyUpdateFileLog

import logging
import sys

from lib_land_registry_download.lib_constants.process_name import PROCESS_NAME_LAND_REGISTRY_MONTHLY_UPDATE_SHA256_CALCULATOR as PROCESS_NAME
from lib_land_registry_download.lib_constants.process_name import OLD_PROCESS_NAME_LAND_REGISTRY_MONTHLY_UPDATE_SHA256_CALCULATOR as CLIENT_ID
from lib_land_registry_download.lib_constants.process_name import OLD_PROCESS_NAME_LAND_REGISTRY_MONTHLY_UPDATE_SHA256_CALCULATOR as GROUP_ID
from lib_land_registry_download.lib_constants.process_name import PROCESS_NAME_LAND_REGISTRY_MONTHLY_UPDATE_DOWNLOADER
from lib_land_registry_download.lib_constants.process_name import OLD_PROCESS_NAME_LAND_REGISTRY_MONTHLY_UPDATE_DOWNLOADER
from lib_land_registry_download.lib_constants.notification_type import DAILY_DOWNLOAD_MONTHLY_UPDATE_COMPLETE
from lib_land_registry_download.lib_constants.notification_type import DAILY_DOWNLOAD_MONTHLY_UPDATE_SHA256SUM_COMPLETE


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

    consumer = create_consumer(
        bootstrap_servers=f'',
        client_id=CLIENT_ID,
        group_id=GROUP_ID,
    )

    producer = create_producer(
        bootstrap_servers=f'',
        client_id=CLIENT_ID,
    )

    run_process(consumer, producer)


def run_process(
    consumer: Consumer,
    producer: Producer,
) -> None:

    consumer.subscribe([topic_name_land_registry_download_monthly_update_downloader_notification])

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
            document = jsons.loads(
                message.value().decode(),
                MonthlyUpdateDownloadCompleteNotificationDTO,
            )

            try:
                notification_source = document.notification_source

                if (
                    notification_source == OLD_PROCESS_NAME_LAND_REGISTRY_MONTHLY_UPDATE_DOWNLOADER or
                    notification_source == PROCESS_NAME_LAND_REGISTRY_MONTHLY_UPDATE_DOWNLOADER
                ):
                    notification_type = document.notification_type

                    if notification_type == DAILY_DOWNLOAD_MONTHLY_UPDATE_COMPLETE:

                        thread_handle = threading.Thread(target=consumer_poll_loop, args=(consumer,))
                        thread_handle.start()

                        filename = document.filename
                        log.info(f'run_process: calculate_sha256sum_and_notify filename={filename}')
                        try:
                            sha256sum = calculate_sha256sum(filename)

                            if sha256sum is not None:
                                log_message = f'{datetime.now(timezone.utc)}: sha256sum of file {filename} is {sha256sum}'
                                log.info(log_message)

                                if update_database(producer, filename, sha256sum, timestamp=document.timestamp) == True:
                                    pass
                                else:
                                    #notify(producer, filename, sha256sum)
                                    pass
                                    # what to do? TODO
                                    # this now can only happen if the hashlib algorithm fails, and does not raise
                                    # an exception

                                notify(producer, filename, sha256sum)
                            else:
                                log_message = f'{datetime.now(timezone.utc)}: failed to calculate sha256sum of file {filename}'
                                log.error(log_message)

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


def calculate_sha256sum(
    filename: str,
) -> str|None:
    file_path = get_file_path(filename)

    if not os.path.exists(file_path):
        log_message = f'{file_path} does not exist'
        log.error(log_message)
        raise FileNotFoundError(log_message)

    if not os.path.isfile(file_path):
        log_message = f'{file_path} is not a file'
        log.error(log_message)
        raise PathIsNotAFileError(file_path, log_message)

    with open(file_path, 'rb', buffering=0) as f:
        sha256sum = hashlib.file_digest(f, 'sha256').hexdigest()
        log_message = f'{file_path} sha256sum is {sha256sum}'
        log.info(log_message)

        return sha256sum

    return None


def notify(producer: Producer, filename: str, sha256sum: str) -> None:

    document = MonthlyUpdateSHA256CalculationCompleteNotificationDTO(
        notification_source=PROCESS_NAME,
        notification_type=DAILY_DOWNLOAD_MONTHLY_UPDATE_SHA256SUM_COMPLETE,
        timestamp=datetime.now(timezone.utc),
        filename=filename,
        sha256sum=sha256sum,
    )

    document_json_str = jsons.dumps(document)

    producer.produce(
        topic=topic_name_land_registry_download_monthly_update_sha256_calculator_notification,
        key=f'no_key',
        value=document_json_str,
    )

    producer.flush()


def update_database(producer: Producer, filename: str, sha256sum: str, timestamp: datetime) -> bool:

    log.info(
        f'update_database: filename={filename}, sha256sum={sha256sum}, timestamp={timestamp}'
    )

    url = 'postgresql://user:password@host/postgres'
    engine_postgres = create_engine(url)

    with Session(engine_postgres) as session:

        row = session.query(PricePaidDataMonthlyUpdateFileLog) \
            .filter_by(filename=filename) \
            .one_or_none()

        # don't expect to see a pre-existing row
        if row is None:
            log.info(
                f'no previously existing row with filename {filename}, create new row'
            )

            new_row = PricePaidDataMonthlyUpdateFileLog(
                filename=filename,
                sha256sum=sha256sum,
                created_datetime=timestamp,
            )
            session.add(new_row)
            session.commit()
            return True
        else:
            log.info(
                f'existing row with filename {filename}'
            )

            if row.sha256sum != sha256sum:
                # row.sha256sum = sha256sum
                # session.commit()
                # this is an unexpected case, just error
                log.error(
                    (
                        f'database table {PricePaidDataMonthlyUpdateFileLog.__tablename__} '
                        f'row with filename={filename} has sha256={row.sha256sum}, '
                        f'but calculated value is {sha256sum}'
                    )
                )
            else:
                log.error(
                    (
                        f'database table {PricePaidDataMonthlyUpdateFileLog.__tablename__} '
                        f'row with filename={filename} already exists'
                    )
                )
            return False


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


