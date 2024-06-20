#!/usr/bin/env python3

import jsons
import pandas
from datetime import datetime
from datetime import timezone

from sqlalchemy import Connection
from sqlalchemy import create_engine
from sqlalchemy import text

import signal
import os
import threading

from lib_kafka_land_registry_download import create_consumer
from lib_kafka_land_registry_download import create_producer

from confluent_kafka import Consumer
from confluent_kafka import Producer

from lib_topic_name import topic_name_land_registry_download_downloader_notification

from lib_dto_land_registry_download import LandRegistryDownloadTriggerNotificationDTO
from lib_dto_land_registry_download import LandRegistryDownloadTriggerNotificationDTO


event_thead_terminate = threading.Event()


def main():

    PROCESS_NAME = 'daily_download_database_uploader'

    consumer = create_consumer(
        bootstrap_servers=f'',
        client_id=PROCESS_NAME,
        group_id=PROCESS_NAME,
    )

    producer = create_producer(
        bootstrap_servers=f'',
        client_id=PROCESS_NAME,
    )

    run_database_upload_process(consumer, producer)


def run_database_upload_process(
    consumer: Consumer,
    producer: Producer,
) -> None:

    consumer.subscribe([topic_name_land_registry_download_downloader_notification])

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
            print(f'{message.value().decode()}')
            raise RuntimeError(f'{message.value().decode()}')
        else:
            document = jsons.loads(
                message.value().decode(),
                LandRegistryDownloadTriggerNotificationDTO,
            )

            try:
                notification_source = document.notification_source
                if notification_source == 'daily_download_downloader':
                    notification_type = document.notification_type

                    if notification_type == 'daily_download_complete':
                        filename = document.filename

                        thread_handle = threading.Thread(target=consumer_poll_loop, args=(consumer,))
                        thread_handle.start()

                        run_uploader(
                            producer=producer,
                            filename=filename,
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
                log_kafka_exception(producer, exception)
                print(f'{exception}')


def consumer_poll_loop(consumer: Consumer) -> None:
    print('consumer_poll_loop: consumer_poll_loop starts')

    print('consumer_poll_loop: pausing consumer')
    topic_partition_assignment = consumer.assignment()
    consumer.pause(topic_partition_assignment)
    print('consumer_poll_loop: consumer paused')

    while True:
        consumer_short_poll_duration = 1.0
        message = consumer.poll(consumer_short_poll_duration)

        if message is not None:
            raise RuntimeError(f'consumer abort')

        if event_thead_terminate.is_set():
            print(f'consumer_poll_loop: event set')
            break

    print(f'consumer_poll_loop: resuming consumer')
    consumer.resume(topic_partition_assignment)
    print(f'consumer_poll_loop: consumer resumed')


def run_uploader(
    producer: Producer,
    filename: str,
):

    if os.path.exists(filename):
        if os.path.isfile(filename):

            upload_time_start = datetime.now(timezone.utc)

            with open(filename, 'rb') as ifile:

                df = read_dataframe_and_process()

                # TODO: should we perform the cleanup process for PAON?

                update_database(df)

            upload_time_end = datetime.now(timezone.utc)
            upload_time = upload_time_end - upload_time_start

        else:
            log_message = f'{filename} is not a file'
    else:
        log_message = f'path {filename} does not exist'
        log_kafka_error(producer, log_message)



def update_database(df: pandas.DataFrame) -> None:

    url = 'postgresql://user:password@host/postgres'
    engine_postgres = create_engine(url)

    with engine_postgres.connect() as connection:

        transaction_unique_id_set = get_transaction_unique_id_set()

        df_len_before = len(df)
        df = df[~df['transaction_unique_id'].isin(transaction_unique_id_set)]
        df_len_after = len(df)
        df_number_of_rows_removed = df_len_before - df_len_after
        df_number_of_rows_remaining = df_len_after

        # TODO: could optimize using df.to_sql

        for index, row in df.iterrows():
            query_string = text(
                '''
                    insert into land_registry.price_paid_data (
                        transaction_unique_id,
                        price,
                        transaction_date,
                        postcode,
                        property_type,
                        new_tag,
                        lease,
                        primary_address_object_name,
                        secondary_address_object_name,
                        street,
                        locality,
                        town_city,
                        district,
                        county,
                        ppd_cat,
                        record_status
                    ) values (
                        :transaction_unique_id,
                        :price,
                        :transaction_date,
                        :postcode,
                        :property_type,
                        :new_tag,
                        :lease,
                        :primary_address_object_name,
                        :secondary_address_object_name,
                        :street,
                        :locality,
                        :town_city,
                        :district,
                        :county,
                        :ppd_cat,
                        :record_status
                    )
                '''
            )

            query_params = {
                'transaction_unique_id': row['transaction_unique_id'],
                'price': row['price'],
                'transaction_date': row['transaction_date'],
                'postcode': row['postcode'],
                'property_type': row['property_type'],
                'new_tag': row['new_tag'],
                'lease': row['lease'],
                'primary_address_object_name': row['primary_address_object_name'],
                'secondary_address_object_name': row['secondary_address_object_name'],
                'street': row['street'],
                'locality': row['locality'],
                'town_city': row['town_city'],
                'district': row['district'],
                'county': row['county'],
                'ppd_cat': row['ppd_cat'],
                'record_status': row['record_status'],
            }

            connection.execute(query_string, query_params)

        # TODO: query_log table, created_datetime, observed_datetime

        connection.commit()

    # before finishing this function, need to know why the data in the db
    # appears to differ from the downloaded dataset
    #
    # there are 28919900 rows in the file
    #
    # select count(transaction_unique_id) from land_registry.price_paid_data ppd;               -- 28951515
    # select count(transaction_unique_id) from land_registry.price_paid_data_raw ppdr; 			-- 28919900
    #
    # select count(distinct transaction_unique_id) from land_registry.price_paid_data ppd;		-- 28951515
    # select count(distinct transaction_unique_id) from land_registry.price_paid_data_raw ppdr; -- 28919900




def get_transaction_unique_id_set(connection: Connection):

    transaction_unique_id_set = set()

    query_string = text(
        '''
            select transaction_unique_id
            from land_registry.price_paid_data
        '''
    )

    query_params = {}

    query_result = connection.execute(query_string, query_params)

    for row in query_result:
        transaction_unique_id = row.transaction_unique_id

        if transaction_unique_id in transaction_unique_id_set:
            transaction_unique_id_set.add(transaction_unique_id)
        else:
            raise RuntimeError(f'duplicate transaction unique id: {transaction_unique_id}')

    return transaction_unique_id_set


def read_dataframe_and_process(ifile) -> pandas.DataFrame:

    df = pandas.read_csv(
        ifile,
        sep=',',
        quotechar='"',
        dtype=str,
        header=None,
    )

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
