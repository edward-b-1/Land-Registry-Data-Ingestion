#!/usr/bin/env python3

import logging
import sys
import os

from lib_land_registry_download.lib_db import PricePaidData

from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy import Engine

import argparse
import pandas
from datetime import datetime
from datetime import timezone


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


def process_args():
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
    parser.add_argument(
        '--fix-database',
        action=argparse.BooleanOptionalAction,
    )
    args = parser.parse_args()
    return args


def main():
    postgres_address = os.environ['POSTGRES_ADDRESS']
    postgres_user = os.environ['POSTGRES_USER']
    postgres_password = os.environ['POSTGRES_PASSWORD']
    postgres_database = os.environ['POSTGRES_DATABASE']
    postgres_connection_string = f'postgresql://{postgres_user}:{postgres_password}@{postgres_address}/{postgres_database}'

    #url = 'postgresql://user:password@host/postgres'
    engine_postgres = create_engine(postgres_connection_string)

    args = process_args()

    input_file = args.input_file
    fix_database = args.fix_database

    print(f'input_file={input_file}')
    print(f'fix_database={fix_database}')


    if os.path.exists('df_merged_file_only_merged_copy.csv'):
        print(f'file df_merged_file_only_merged_copy.csv exists, loading file')
        df_merged_file_only_merged_copy = pandas.read_csv('df_merged_file_only_merged_copy.csv')
        df_merged_file_only_merged_copy = format_dataframe(df_merged_file_only_merged_copy)
    else:
        print(f'file df_merged_file_only_merged_copy.csv does not exist, loading data from source')

        limit=None

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
        df_copy = df.copy()[['transaction_unique_id', 'record_status']]
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
        if limit is not None:
            df = df.iloc[0:limit-1]
        df.reset_index(inplace=True, drop=True)
        print(f'data loaded from file:')
        print(df)
        print(df.columns)

        if limit is not None:
            query = text(
                f'''
                    select * from land_registry.price_paid_data
                    where is_deleted = 'F'
                    order by transaction_unique_id asc
                    limit {limit}
                '''
            )
        else:
            query = text(
                f'''
                    select * from land_registry.price_paid_data
                    where is_deleted = 'F'
                    order by transaction_unique_id asc
                '''
            )

        df_db = pandas.read_sql(
            con=engine_postgres,
            sql=query,
        ).drop(
            columns=[
                'price_paid_data_id',
                'is_deleted',
                'created_datetime',
                'updated_datetime',
                'deleted_datetime',
                'created_datetime_original',
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
        print(f'data loaded from database:')
        print(df_db)

        #print(df.dtypes)
        #print(df_db.dtypes)

        print(f'type(df_db.columns):')
        print(type(df_db.columns))
        on_columns = list(df_db.columns)

        # print(df.iloc[6033:6037])
        # print(df_db.iloc[6033:6037])

        df_merged = df_db.merge(
            df,
            on=on_columns,
            how='outer',
            indicator=True,
        )

        print(f'merged dataframe:')
        print(df_merged)

        if set(df.columns) != set(df_db.columns):
            print(f'columns in dataframes do not match')
            print(f'columns from {input_file}:')
            print(df.columns)
            print(f'columns from database:')
            print(df_db.columns)

        df_merged_database_only = df_merged[df_merged['_merge'] == 'left_only']
        df_merged_file_only = df_merged[df_merged['_merge'] == 'right_only']
        df_merged_both = df_merged[df_merged['_merge'] == 'both']

        count_database = len(df_merged_database_only)
        count_file = len(df_merged_file_only)
        count_both = len(df_merged_both)

        print(f'number of records in database only: {count_database}')
        print(f'number of records in file only: {count_file}')
        print(f'number of records in both: {count_both}')

        print(f'df_merged_database_only:')
        print(df_merged_database_only)
        print(f'df_merged_file_only')
        print(df_merged_file_only)

        df_merged_database_only.to_csv(
            'df_db_only.csv',
            index=False,
        )
        df_merged_file_only.to_csv(
            'df_file_only.csv',
            index=False,
        )

        df_merged_file_only_merged_copy = df_merged_file_only.merge(
            df_copy,
            on=['transaction_unique_id'],
            how='inner',
            indicator=False,
        )
        print(f'df_merged_file_only_merged_copy:')
        print(df_merged_file_only_merged_copy)

        df_merged_file_only_merged_copy.to_csv(
            'df_merged_file_only_merged_copy.csv',
            index=False,
        )

    if fix_database:
        function_fix_database(engine_postgres, df_merged_file_only_merged_copy)

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


def function_fix_database(
    engine_postgres: Engine,
    df_merged_file_only_merged_copy: pandas.DataFrame,
):
    print(f'len={len(df_merged_file_only_merged_copy)}')

    now = datetime.now(timezone.utc)

    with engine_postgres.connect() as connection:
        for index, row in df_merged_file_only_merged_copy.iterrows():
            print(f'row={row}')
            query = text(
                f'''
                    select transaction_unique_id
                    from land_registry.price_paid_data
                    where transaction_unique_id = :transaction_unique_id
                '''
            )
            params = {
                'transaction_unique_id': row.transaction_unique_id,
            }
            result = connection.execute(query, params)
            # print(type(result))
            # print(result)
            # for row in result:
            #     print(type(row))
            #     print(row)
            #     print(row[0])
            # continue
            result_row = result.one_or_none()
            if result_row is not None:
                transaction_unique_id = result_row[0]

                query = text(
                    f'''
                        update land_registry.price_paid_data
                        set
                            price = :price,
                            transaction_date = :transaction_date,
                            postcode = :postcode,
                            property_type = :property_type,
                            new_tag = :new_tag,
                            lease = :lease,
                            primary_address_object_name = :primary_address_object_name,
                            secondary_address_object_name = :secondary_address_object_name,
                            street = :street,
                            locality = :locality,
                            town_city = :town_city,
                            district = :district,
                            county = :county,
                            ppd_cat = ppd_cat,
                            record_status = :record_status,
                            is_deleted = :is_deleted,
                            updated_datetime = :updated_datetime
                        where
                            transaction_unique_id = :transaction_unique_id
                    '''
                )
                params = {
                    'transaction_unique_id': row.transaction_unique_id,
                    'price': row.price,
                    'transaction_date': row.transaction_date,
                    'postcode': row.postcode,
                    'property_type': row.property_type,
                    'new_tag': row.new_tag,
                    'lease': row.lease,
                    'primary_address_object_name': row.primary_address_object_name,
                    'secondary_address_object_name': row.secondary_address_object_name,
                    'street': row.street,
                    'locality': row.locality,
                    'town_city': row.town_city,
                    'district': row.district,
                    'county': row.county,
                    'ppd_cat': row.ppd_cat,
                    'record_status': row.record_status,
                    'is_deleted': False,
                    'updated_datetime': now,
                }
                result = connection.execute(query, params)
                connection.commit()
            else:
                query = text(
                    f'''
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
                            record_status,
                            is_deleted,
                            created_datetime
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
                            :record_status,
                            :is_deleted,
                            :created_datetime
                        )
                    '''
                )
                params = {
                    'transaction_unique_id': row.transaction_unique_id,
                    'price': row.price,
                    'transaction_date': row.transaction_date,
                    'postcode': row.postcode,
                    'property_type': row.property_type,
                    'new_tag': row.new_tag,
                    'lease': row.lease,
                    'primary_address_object_name': row.primary_address_object_name,
                    'secondary_address_object_name': row.secondary_address_object_name,
                    'street': row.street,
                    'locality': row.locality,
                    'town_city': row.town_city,
                    'district': row.district,
                    'county': row.county,
                    'ppd_cat': row.ppd_cat,
                    'record_status': row.record_status,
                    'is_deleted': False,
                    'created_datetime': now,
                }
                #try:
                result = connection.execute(query, params)
                connection.commit()
                #except Exception:
                #    print(f'{row.transaction_unique_id} failed')
                #    connection.rollback()


if __name__ == '__main__':
    main()
