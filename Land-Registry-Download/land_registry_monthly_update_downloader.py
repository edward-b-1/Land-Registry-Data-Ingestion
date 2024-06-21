#!/usr/bin/env python3

'''
    TODO:

    This process and the other `daily_download_downloder.py` process needs to
    have a way to fast forward to the end of the topic (consuming all events)
    and then only process the final event. (Skip all notifications to trigger
    a download except the last one.) Then the whole batch should be comitted.

    Without this, the process will download the same data repeatedly for each
    notification document it observes. If a day of data was missed, then
    that data is gone. There is no way to recover it, so it should be ignored.

    Also need to build a process which "primes" the database. It should delete
    the existing database tables, and download the pp-complete.txt data,
    populating a new database table with this data.

    [DONE]
'''

import time
import jsons
from datetime import datetime
from datetime import timedelta
from datetime import timezone

import os
import signal
import requests
import threading

from lib_land_registry_download.lib_kafka import create_consumer
from lib_land_registry_download.lib_kafka import create_producer

from confluent_kafka import Consumer
from confluent_kafka import Producer

from lib_land_registry_download.lib_topic_name import topic_name_land_registry_download_controller_notification
from lib_land_registry_download.lib_topic_name import topic_name_land_registry_download_monthly_update_downloader_notification

# TODO: what is this? use it here? it came from daily_download_controller.py
#from lib_kafka_logger import KafkaLogger

from lib_land_registry_download.lib_dto import CronTriggerNotificationDTO
from lib_land_registry_download.lib_dto import MonthlyUpdateDownloadCompleteNotificationDTO

import logging
import sys

from lib_land_registry_download.lib_constants.process_name import PROCESS_NAME_LAND_REGISTRY_MONTHLY_UPDATE_DOWNLOADER as PROCESS_NAME
from lib_land_registry_download.lib_constants.process_name import OLD_PROCESS_NAME_LAND_REGISTRY_MONTHLY_UPDATE_DOWNLOADER as GROUP_ID
from lib_land_registry_download.lib_constants.process_name import OLD_PROCESS_NAME_LAND_REGISTRY_MONTHLY_UPDATE_DOWNLOADER as CLIENT_ID
from lib_land_registry_download.lib_constants.process_name import PROCESS_NAME_LAND_REGISTRY_CRON_TRIGGER
from lib_land_registry_download.lib_constants.process_name import OLD_PROCESS_NAME_LAND_REGISTRY_CRON_TRIGGER
from lib_land_registry_download.lib_constants.notification_type import DAILY_DOWNLOAD_TRIGGER
from lib_land_registry_download.lib_constants.notification_type import DAILY_DOWNLOAD_MONTHLY_UPDATE_COMPLETE


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

    run_download_process(consumer, producer)


def process_message_queue(
    consumer: Consumer,
    producer: Producer,
    message_queue: list,
):

    def filter_function(message) -> bool:
        notification_source = message.notification_source
        if (
            notification_source == OLD_PROCESS_NAME_LAND_REGISTRY_CRON_TRIGGER or
            notification_source == PROCESS_NAME_LAND_REGISTRY_CRON_TRIGGER
        ):
            notification_type = message.notification_type
            if notification_type == DAILY_DOWNLOAD_TRIGGER:
                return True
        return False

    message_queue = \
        list(
            filter(
                filter_function,
                message_queue
            )
        )

    if len(message_queue) > 0:
        message = message_queue[-1]

        notification_source = message.notification_source
        if (
            notification_source == OLD_PROCESS_NAME_LAND_REGISTRY_CRON_TRIGGER or
            notification_source == PROCESS_NAME_LAND_REGISTRY_CRON_TRIGGER
        ):
            notification_type = message.notification_type
            if notification_type == DAILY_DOWNLOAD_TRIGGER:

                thread_handle = threading.Thread(target=consumer_poll_loop, args=(consumer,))
                thread_handle.start()

                log.info('run_download_process: run_download_and_notify')
                run_download_return_value = run_download()

                if run_download_return_value is not None:
                    (filename, download_timestamp, download_time) = run_download_return_value

                    notify(
                        producer,
                        filename=filename,
                        document=message,
                        timestamp_download=download_timestamp,
                    )

                    log_message = f'{datetime.now(timezone.utc)}: finished downloading file: {filename} ({download_time})'
                    log.info(log_message)
                else:
                    log_message = f'{datetime.now(timezone.utc)}: failed to download file: {filename}, ({download_time})'
                    log.error(log_message)

                event_thead_terminate.set()
                thread_handle.join()
                consumer.commit()
                event_thead_terminate.clear()

            else:
                raise RuntimeError(f'unknown notification type: {notification_type}')
        else:
            raise RuntimeError(f'unknown notification source: {notification_source}')


def run_download_process(
    consumer: Consumer,
    producer: Producer,
) -> None:

    consumer.subscribe([topic_name_land_registry_download_controller_notification])
    consumer_poll_timeout = 10.0
    message_queue = []

    global exit_flag

    while not exit_flag:

        # provide a short period flush to allow the producer to catch up
        # with dispatched events
        producer.flush(3.0)
        producer.poll(1.0)
        message = consumer.poll(consumer_poll_timeout)

        if message is None:
            process_message_queue(consumer, producer, message_queue)
            message_queue.clear()
            continue

        if message.error():
            log_message = f'kafka message error: {message.value().decode()}'
            log.error(log_message)
            raise RuntimeError(f'{message.value().decode()}')
        else:
            document = jsons.loads(
                message.value().decode(),
                CronTriggerNotificationDTO,
            )

            try:
                notification_source = document.notification_source

                if (
                    notification_source == OLD_PROCESS_NAME_LAND_REGISTRY_CRON_TRIGGER or
                    notification_source == PROCESS_NAME_LAND_REGISTRY_CRON_TRIGGER
                ):
                    notification_type = document.notification_type

                    if notification_type == DAILY_DOWNLOAD_TRIGGER:
                        message_queue.append(document)

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


def run_download() -> tuple[str, timedelta]|None:

    def run_download_internal_logic() -> tuple[str|None, timedelta]|None:
        url = 'http://prod1.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-monthly-update.txt'

        download_time_start = datetime.now(timezone.utc)
        log.info(f'run_download: {download_time_start}, download start url={url}')

        (filename, download_timestamp) = download_data(
            url,
            get_filename(),
        )

        download_time_end = datetime.now(timezone.utc)
        download_time = download_time_end - download_time_start
        log.info(f'run_download: download finished, time = {download_time}')
        return (filename, download_timestamp, download_time)

    fail_count = 0
    while True:
        (filename, download_timestamp, download_time) = run_download_internal_logic()
        if filename is not None:
            return (filename, download_timestamp, download_time)
        else:
            fail_count += 1
            if fail_count > 20:
                log.error(f'download failed after {fail_count} retries, give up')
                return None
            else:
                log.warning(f'download failed, retry in 1h, number of failures: {fail_count}')
            time_1_hour = 3600
            time.sleep(time_1_hour)


def notify(
    producer: Producer,
    filename: str,
    document: CronTriggerNotificationDTO,
    timestamp_download: datetime,
) -> None:

    document = MonthlyUpdateDownloadCompleteNotificationDTO(
        notification_source=PROCESS_NAME,
        notification_type=DAILY_DOWNLOAD_MONTHLY_UPDATE_COMPLETE,
        filename=filename,
        timestamp=datetime.now(timezone.utc),
        timestamp_cron_trigger=document.timestamp_cron_trigger,
        timestamp_download=timestamp_download,
    )

    document_json_str = jsons.dumps(document)

    producer.produce(
        topic=topic_name_land_registry_download_monthly_update_downloader_notification,
        key=f'no_key',
        value=document_json_str,
    )

    producer.flush()


# TODO: inspect the file and see if things like the str.replace(',') function is still needed


def get_filename() -> str:
    filename_base = f'pp-monthly-update-{datetime.now(timezone.utc).date()}'
    filename_extension = 'txt'

    def create_filename(filename_counter=None):
        filename = None
        if filename_counter is None:
            filename = f'{filename_base}.{filename_extension}'
        else:
            filename = f'{filename_base}_{filename_counter}.{filename_extension}'
        return filename

    filename = create_filename()
    file_path = get_file_path(filename)

    filename_counter = 0
    while os.path.exists(file_path):
        filename = create_filename(filename_counter)
        file_path = get_file_path(filename)
        filename_counter += 1

    return filename


def get_file_path(filename: str) -> str:
    data_directory = '/data-land-registry/pp-monthly-update'
    return f'{data_directory}/{filename}'


def download_data(url: str, filename: str) -> tuple[str, datetime]|None:

    download_timestamp = datetime.now(timezone.utc)
    log_message = f'{download_timestamp}: download starting: {filename}, {url}'
    log.info(log_message)

    request = requests.get(url, allow_redirects=True)

    if request.status_code == 200:
        log_message = f'request status 200: {url}'
        log.info(log_message)
    else:
        log_message = f'request failure: status_code={request.status_code}'
        log.error(log_message)
        return None

    file_path = get_file_path(filename)
    log.info(f'saving download content to file {file_path}')

    with open(file_path, 'wb') as ofile:
        try:
            ofile.write(request.content)
            ofile.flush()
        except Exception as exception:
            log_message = f'{str(exception)}'
            log.error(log_message)
            return None

    log_message = f'{datetime.now(timezone.utc)}: download complete: {filename}'
    log.info(log_message)

    return (filename, download_timestamp)


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
