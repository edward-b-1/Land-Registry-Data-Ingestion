#!/usr/bin/env python3

'''
    This process takes a single `pp-complete.txt` file and initializes the database table
    land_registry.price_paid_data.
'''

import os
import argparse
import pandas

from datetime import datetime
from datetime import timezone

from sqlalchemy import create_engine
from sqlalchemy import text


def parse_args():

    parser = argparse.ArgumentParser(
        prog='database-initializer',
        description='Land Registry Data Database Initializer',
    )

    parser.add_argument('--input-file', action='store', metavar='input-file', dest='input_file', required=True)

    args = parser.parse_args()

    return args


def parse_args_get_input_filename(args) -> str:

    input_filename = args.input_file
    return input_filename


def format_dataframe(df: pandas.DataFrame, datetime_now: datetime):

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

    df['is_deleted'] = False
    df['created_datetime'] = datetime_now
    df['updated_datetime'] = None
    df['deleted_datetime'] = None

    return df


def main():
    postgres_address = os.environ['POSTGRES_ADDRESS']
    postgres_user = os.environ['POSTGRES_USER']
    postgres_password = os.environ['POSTGRES_PASSWORD']
    postgres_database = os.environ['POSTGRES_DATABASE']
    postgres_connection_string = f'postgresql://{postgres_user}:{postgres_password}@{postgres_address}/{postgres_database}'
    # TODO: move

    args = parse_args()
    input_filename = parse_args_get_input_filename(args)

    if not os.path.exists(input_filename):
        print(f'{input_filename} does not exist')
    else:
        if not os.path.isfile(input_filename):
            print(f'{input_filename} is not a file')
        else:
            df = pandas.read_csv(
                input_filename,
                sep=',',
                quotechar='"',
                dtype=str,
                header=None,
                keep_default_na=False,
                #nrows=100,
            )

            datetime_now = datetime.now(timezone.utc)

            df = format_dataframe(df, datetime_now)

            #url = 'postgresql://user:password@host/postgres'
            engine_postgres = create_engine(postgres_connection_string)

            with engine_postgres.connect() as connection:

                query_string = text(
                    '''
                        delete from land_registry.price_paid_data
                    '''
                )

                query_params = {}

                connection.execute(query_string, query_params)
                connection.commit()

                df.to_sql(
                    con=engine_postgres,
                    schema='land_registry',
                    name='price_paid_data',
                    if_exists='append',
                    index_label='price_paid_data_id',
                    #chunksize=1,
                )

                query_string = text(
                    '''
                        insert into
                            land_registry.price_paid_data_log
                            (
                                log_timestamp,
                                query_row_count_total,
                                query_row_count_inserted,
                                query_row_count_updated,
                                query_row_count_deleted,
                                row_count_before,
                                row_count_after
                            )
                        values
                            (
                                :log_timestamp,
                                :query_row_count_total,
                                :query_row_count_inserted,
                                :query_row_count_updated,
                                :query_row_count_deleted,
                                :row_count_before,
                                :row_count_after
                            )
                    '''
                )

                query_params = {
                    'log_timestamp': datetime_now,
                    'query_row_count_total': len(df),
                    'query_row_count_inserted': len(df),
                    'query_row_count_updated': 0,
                    'query_row_count_deleted': 0,
                    'row_count_before': 0,
                    'row_count_after': len(df),
                }

                connection.execute(query_string, query_params)
                connection.commit()


if __name__ == '__main__':
    main()
