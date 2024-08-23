
import jsons
from datetime import datetime
from datetime import timezone

import signal
import threading

from lib_land_registry_data.lib_kafka import create_consumer
from lib_land_registry_data.lib_kafka import create_producer

from confluent_kafka import Consumer
from confluent_kafka import Producer

from lib_land_registry_data.lib_topic_name import topic_name_land_registry_data_monthly_update_sha256_calculator_notification
from lib_land_registry_data.lib_topic_name import topic_name_land_registry_data_monthly_update_data_decision_notification

from lib_land_registry_data.lib_dto import MonthlyUpdateSHA256CalculationCompleteNotificationDTO
from lib_land_registry_data.lib_dto import MonthlyUpdateDataDecisionCompleteNotificationDTO

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from lib_land_registry_data.lib_db import PricePaidDataMonthlyUpdateFileLog

import logging
import sys
import os

from lib_land_registry_data.lib_constants.process_name import PROCESS_NAME_LAND_REGISTRY_MONTHLY_UPDATE_DATA_DECISION as PROCESS_NAME
from lib_land_registry_data.lib_constants.process_name import OLD_PROCESS_NAME_LAND_REGISTRY_MONTHLY_UPDATE_DATA_DECISION as GROUP_ID
from lib_land_registry_data.lib_constants.process_name import OLD_PROCESS_NAME_LAND_REGISTRY_MONTHLY_UPDATE_DATA_DECISION as CLIENT_ID
from lib_land_registry_data.lib_constants.process_name import PROCESS_NAME_LAND_REGISTRY_MONTHLY_UPDATE_SHA256_CALCULATOR
from lib_land_registry_data.lib_constants.process_name import OLD_PROCESS_NAME_LAND_REGISTRY_MONTHLY_UPDATE_SHA256_CALCULATOR
from lib_land_registry_data.lib_constants.notification_type import DAILY_DOWNLOAD_MONTHLY_UPDATE_SHA256SUM_COMPLETE
from lib_land_registry_data.lib_constants.notification_type import DAILY_DOWNLOAD_MONTHLY_UPDATE_DATA_DECISION_COMPLETE


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

    consumer.subscribe([topic_name_land_registry_data_monthly_update_sha256_calculator_notification])

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
                MonthlyUpdateSHA256CalculationCompleteNotificationDTO,
            )

            try:
                notification_source = dto.notification_source

                if (
                    notification_source == OLD_PROCESS_NAME_LAND_REGISTRY_MONTHLY_UPDATE_SHA256_CALCULATOR or
                    notification_source == PROCESS_NAME_LAND_REGISTRY_MONTHLY_UPDATE_SHA256_CALCULATOR
                ):
                    notification_type = dto.notification_type

                    if notification_type == DAILY_DOWNLOAD_MONTHLY_UPDATE_SHA256SUM_COMPLETE:

                        thread_handle = threading.Thread(target=consumer_poll_loop, args=(consumer,))
                        thread_handle.start()

                        filename = dto.filename
                        sha256sum = dto.sha256sum
                        assert len(sha256sum) > 0
                        print(f'run_process: data decision: filename={filename}')

                        decision = data_decision(filename)

                        if decision == 'ignore':
                            log_message = f'ignoring file {filename}, duplicate sha256sum {sha256sum}'
                            log.info(log_message)
                            update_database(filename, 'ignored')
                        elif decision == 'process':
                            log_message = f'processing file {filename}, sha256sum {sha256sum} differs'
                            log.info(log_message)
                            update_database(filename, 'processed')

                        decision_map = {
                            'ignore': 'ignored',
                            'process': 'processed',
                        }

                        notify(
                            producer=producer,
                            filename=filename,
                            sha256sum=sha256sum,
                            data_decision=decision_map[decision],
                            sha_calculation_dto=dto,
                        )

                        event_thead_terminate.set()
                        thread_handle.join()
                        consumer.commit()
                        event_thead_terminate.clear()

                    else:
                        raise RuntimeError(f'unknown notification type: {notification_type}')
                else:
                    raise RuntimeError(f'unknown notification source: {notification_source}')

            except Exception as exception:
                log.error(exception)

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


def data_decision(
    filename: str,
) -> str|None:
    # TODO: move
    postgres_address = os.environ['POSTGRES_ADDRESS']
    postgres_user = os.environ['POSTGRES_USER']
    postgres_password = os.environ['POSTGRES_PASSWORD']
    postgres_database = os.environ['POSTGRES_DATABASE']
    postgres_connection_string = f'postgresql://{postgres_user}:{postgres_password}@{postgres_address}/{postgres_database}'

    #url = 'postgresql://user:password@host/postgres'
    engine_postgres = create_engine(postgres_connection_string)

    with Session(engine_postgres) as session:
        rows = (
            session
            .query(PricePaidDataMonthlyUpdateFileLog)
            .filter_by(filename=filename)
            .all()
        )

        if len(rows) > 1:
            log_message = f'multiple rows found in database with filename={filename}'
            raise RuntimeError(log_message)
        elif len(rows) == 0:
            log_message = f'missing row in database for filename={filename}'
            raise RuntimeError(log_message)
        else:
            row = rows[0]
            sha256sum = row.sha256sum

            # get the last processed row
            # where the process decision was "processed" (not "ignored")

            processed_rows = (
                session
                .query(PricePaidDataMonthlyUpdateFileLog)
                .filter_by(process_decision='processed')
                .order_by(PricePaidDataMonthlyUpdateFileLog.processed_datetime)
                .all()
            )

            if len(processed_rows) == 0:
                # no previous processed rows
                return 'process'
            elif len(processed_rows) > 0:
                last_processed_row = processed_rows[-1]
                last_sha256sum = last_processed_row.sha256sum

                if last_sha256sum != sha256sum:
                    # previously processed file has a different sha256
                    return 'process'
                else:
                    return 'ignore'


def notify(
    producer: Producer,
    filename: str,
    sha256sum: str,
    data_decision: str,
    sha_calculation_dto: MonthlyUpdateSHA256CalculationCompleteNotificationDTO,
) -> None:
    now = datetime.now(timezone.utc)

    data_decision_dto = MonthlyUpdateDataDecisionCompleteNotificationDTO(
        notification_source=PROCESS_NAME,
        notification_type=DAILY_DOWNLOAD_MONTHLY_UPDATE_DATA_DECISION_COMPLETE,
        timestamp=now,
        filename=filename,
        sha256sum=sha256sum,
        data_decision=data_decision,
        timestamp_cron_trigger=sha_calculation_dto.timestamp_cron_trigger,
        timestamp_download=sha_calculation_dto.timestamp_download,
        timestamp_shasum=sha_calculation_dto.timestamp_shasum,
        timestamp_data_decision=now,
    )

    document_json_str = jsons.dumps(data_decision_dto)

    producer.produce(
        topic=topic_name_land_registry_data_monthly_update_data_decision_notification,
        key=f'no_key',
        value=document_json_str,
    )

    producer.flush()


def update_database(filename: str, data_decision: str) -> bool:
    log.info(f'update_database: filename={filename}, data_decision={data_decision}')

    # TODO: move
    postgres_address = os.environ['POSTGRES_ADDRESS']
    postgres_user = os.environ['POSTGRES_USER']
    postgres_password = os.environ['POSTGRES_PASSWORD']
    postgres_database = os.environ['POSTGRES_DATABASE']
    postgres_connection_string = f'postgresql://{postgres_user}:{postgres_password}@{postgres_address}/{postgres_database}'

    #url = 'postgresql://user:password@host/postgres'
    engine_postgres = create_engine(postgres_connection_string)

    with Session(engine_postgres) as session:

        row = session.query(PricePaidDataMonthlyUpdateFileLog) \
            .filter_by(filename=filename) \
            .one_or_none()

        # expect to see a pre-existing row
        if row is None:
            log_message = f'missing row, filename {filename}'
            log.error(log_message=log_message)
            raise RuntimeError(log_message)
        else:
            log.info(f'updating row with filename {filename} to data_decision={data_decision}')

            row.processed_datetime = datetime.now(timezone.utc)
            row.process_decision = data_decision
            session.commit()


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


