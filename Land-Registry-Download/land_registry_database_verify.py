#!/usr/bin/env python3

import logging
import sys
import os

from lib_land_registry_download.lib_db import PricePaidData

from sqlalchemy import create_engine
from sqlalchemy import text

import argparse
import pandas

PROCESS_NAME = 'land_registry_database_verify'

def format_dataframe_dtypes(df: pandas.DataFrame):
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
    return df


def format_dataframe(df: pandas.DataFrame):
    df = format_dataframe_dtypes(df)
    #df.index.names = ['price_paid_data_id']
    df.fillna('', inplace=True)

    # df['is_deleted'] = False
    # df['created_datetime'] = datetime_now
    # df['updated_datetime'] = None
    # df['deleted_datetime'] = None
    return df


def main():
    postgres_address = os.environ['POSTGRES_ADDRESS']
    postgres_user = os.environ['POSTGRES_USER']
    postgres_password = os.environ['POSTGRES_PASSWORD']
    postgres_database = os.environ['POSTGRES_DATABASE']
    postgres_connection_string = f'postgresql://{postgres_user}:{postgres_password}@{postgres_address}/{postgres_database}'

    limit=100000000

    parser = argparse.ArgumentParser(
        prog=PROCESS_NAME,
        description='Database data verification',
    )
    parser.add_argument(
        '--input-file',
        action='store',
        metavar='input-file',
        dest='input_file',
        required=True,
    )
    args = parser.parse_args()

    input_file = args.input_file

    df = pandas.read_csv(
        input_file,
        sep=',',
        quotechar='"',
        dtype=str,
        header=None,
        keep_default_na=False,
        #nrows=100,
    )
    df = format_dataframe(df)
    df.drop(
        columns = [
            'record_status',
        ],
        inplace=True,
    )
    #print(df)
    df.sort_values(
        by='transaction_unique_id',
        axis=0,
        ascending=True,
        inplace=True,
    )
    #df = df.iloc[0:limit-1]
    df.reset_index(inplace=True, drop=True)
    print(df)
    print(df.columns)

    #url = 'postgresql://user:password@host/postgres'
    engine_postgres = create_engine(postgres_connection_string)

    query_string = text(
        f'''
            select * from land_registry.price_paid_data
            where is_deleted = 'F'
            order by transaction_unique_id asc
            --nlimit {limit}
        '''
    )

    df_db = pandas.read_sql(
        con=engine_postgres,
        sql=query_string,
    ).drop(
        columns=[
            'price_paid_data_id',
            'is_deleted',
            'created_datetime',
            'updated_datetime',
            'deleted_datetime',
        ]
    )
    df_db = format_dataframe_dtypes(df_db)
    df_db.drop(
        columns = [
            'record_status',
        ],
        inplace=True,
    )
    df_db.reset_index(inplace=True, drop=True)
    print(df_db)

    #print(df.dtypes)
    #print(df_db.dtypes)

    print(type(df_db.columns))
    on_columns = list(df_db.columns)

    print(df.iloc[6033:6037])
    print(df_db.iloc[6033:6037])

    df_merged = df_db.merge(
        df,
        on=on_columns,
        how='outer',
        indicator=True,
    )

    print(df_merged)

    if set(df.columns) != set(df_db.columns):
        print(f'columns in dataframes do not match')
        print(f'columns from {input_file}:')
        print(df.columns)
        print(f'columns from database:')
        print(df_db.columns)

    df_merged_left_only = df_merged[df_merged['_merge'] == 'left_only']
    df_merged_right_only = df_merged[df_merged['_merge'] == 'right_only']
    df_merged_both = df_merged[df_merged['_merge'] == 'both']

    count_left = len(df_merged_left_only)
    count_right = len(df_merged_right_only)
    count_both = len(df_merged_both)

    print(f'number of records in database only: {count_left}')
    print(f'number of records in file only: {count_right}')
    print(f'number of records in both: {count_both}')

    print(df_merged_left_only)
    print(df_merged_right_only)

    df_merged_left_only.to_csv(
        'df_db_only.csv',
        index=False,
    )
    df_merged_right_only.to_csv(
        'df_file_only.csv',
        index=False,
    )

    # s1 = df_merged_left_only.iloc[0]
    # s2 = df_merged_right_only.iloc[0]

    # for k, v in s1.items():
    #     print(f'k={k}')
    #     print(f'v={v}')
    #     v2 = s2[k]
    #     if v == v2:
    #         pass
    #         #print(f'equal')
    #     else:
    #         print(f'NOT EQUAL: {type(v)}, {type(v2)}, {v}, {v2}')

    # with engine_postgres.connect() as connection:

    #     query_string = text(
    #         '''
    #             select * from land_registry.price_paid_data
    #             where is_deleted = 'F'
    #         '''
    #     )
    #     query_params = {}
    #     connection.execute(query_string, query_params)

    #     df.to_sql


if __name__ == '__main__':
    main()
