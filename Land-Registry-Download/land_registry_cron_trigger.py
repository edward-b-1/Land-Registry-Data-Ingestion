#!/usr/bin/env python3

from datetime import datetime
from datetime import timedelta
from datetime import timezone

import os
import jsons
import signal
import argparse

from lib_land_registry_download.lib_cron import cron_get_next_schedule
from lib_land_registry_download.lib_cron import cron_get_sleep_time
from lib_land_registry_download.lib_cron import cron_get_sleep_time_timeout
from lib_land_registry_download.lib_cron import cron_do_sleep

from lib_land_registry_download.lib_kafka import create_producer

from confluent_kafka import Producer

from lib_land_registry_download.lib_topic_name import topic_name_land_registry_download_controller_notification

from lib_land_registry_download.lib_dto import CronTriggerNotificationDTO

import logging
import sys

from lib_land_registry_download.lib_constants.process_name import PROCESS_NAME_LAND_REGISTRY_CRON_TRIGGER as PROCESS_NAME
from lib_land_registry_download.lib_constants.process_name import OLD_PROCESS_NAME_LAND_REGISTRY_CRON_TRIGGER as GROUP_ID
from lib_land_registry_download.lib_constants.process_name import OLD_PROCESS_NAME_LAND_REGISTRY_CRON_TRIGGER as CLIENT_ID
from lib_land_registry_download.lib_constants.notification_type import DAILY_DOWNLOAD_TRIGGER


# Processes:
#
# 1. Controller:
#
#   Scheduler for system. Waits on CRON schedule and sends notification
#   to begin download process.
#
# 2. Downloader:
#
#   Waits for notification from Controller and downloads data from Land Registry
#   website. Sends notification when done to Database Uploader.
#
# 3. Database Uploader:
#
#   Waits for notification from Downloader and uploads data from file on disk
#   to database. Performs a reconcilliation process to only upload new rows of
#   data.
#
# 4. Garbage Collector:
#
#   Waits for notification from Database Uploader and removes old files on a
#   schedule based algorithm. Always keeps files with new data. Removes files
#   with duplicated data. The Database Uploader provides this information via
#   the messages sent to Kafka


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


def parse_arguments() -> str:

    # parse arguments
    parser = argparse.ArgumentParser(
        prog = 'LandRegistryDataDownloader',
        description = 'Downloads Land Registry Price Paid Dataset',
    )

    parser.add_argument('--run-now', action = 'store_true')

    args = parser.parse_args()
    return args.run_now


def main():
    log.info(f'{PROCESS_NAME} start')

    log.info(f'read environment variables')
    kafka_bootstrap_servers = os.environ['KAFKA_BOOTSTRAP_SERVERS']
    log.info(f'env: KAFKA_BOOTSTRAP_SERVERS={kafka_bootstrap_servers}')

    log.info(f'parse arguments')
    run_now = parse_arguments()

    log.info(f'create producer')
    producer = create_producer(
        bootstrap_servers=kafka_bootstrap_servers,
        client_id=CLIENT_ID,
    )

    log.info(f'run controller process')
    run_controller_process(producer, run_now)


def run_controller_process(producer: Producer, run_now: bool) -> None:
    first_run = True

    # TODO: shutdown signal
    global exit_flag

    while not exit_flag:
        if first_run and run_now:
            first_run = False
        else:
            log.info(f'{datetime.now(timezone.utc)}: waiting for CRON')

            now = datetime.now(timezone.utc)
            next_schedule = cron_get_next_schedule(now)
            log.info(f'next_schedule = {next_schedule}')

            # only used to log total sleep duration
            sleep_timedelta = cron_get_sleep_time(now, next_schedule)
            log.info(f'sleep for {sleep_timedelta}')

            while True:
                if exit_flag:
                    log.info(f'{datetime.now(timezone.utc)}: process exit')
                    return

                now = datetime.now(timezone.utc)
                if now >= next_schedule:
                    break

                sleep_timedelta = cron_get_sleep_time_timeout(now, next_schedule, timeout=timedelta(seconds=10))
                cron_do_sleep(sleep_timedelta)

        notify_trigger(producer)

        log.info(f'{datetime.now(timezone.utc)}: notification sent')

    log.info(f'{datetime.now(timezone.utc)}: process exit')


def notify_trigger(producer: Producer) -> None:

    now = datetime.now(timezone.utc)

    document = CronTriggerNotificationDTO(
        notification_source=PROCESS_NAME,
        notification_type=DAILY_DOWNLOAD_TRIGGER,
        timestamp=now,
        timestamp_cron_trigger=now,
    )

    document_json_str = jsons.dumps(
        document,
        strip_privates=True,
    )

    producer.produce(
        topic=topic_name_land_registry_download_controller_notification,
        key=f'no_key',
        value=document_json_str
    )

    producer.flush()


exit_flag = False

def ctrl_c_signal_handler(signal, frame):
    print(f'wait for exit...')
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

