#!/usr/bin/env python3

'''
    This process takes a single `pp-complete.txt` file and initializes the database table
    land_registry.price_paid_data.
'''

import io
import argparse
import pandas

from datetime import datetime
from datetime import timezone

from sqlalchemy import Engine
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import boto3
import botocore

PROCESS_NAME = 'land_registry_pp_monthly_data_initialization_service'

from lib_land_registry_data.lib_db import PPCompleteArchiveFileLog

from lib_land_registry_data.lib_env import EnvironmentVariables

from lib_land_registry_data.logging import set_logger_process_name
from lib_land_registry_data.logging import get_logger
from lib_land_registry_data.logging import create_stdout_log_handler
from lib_land_registry_data.logging import create_file_log_handler


set_logger_process_name(
    process_name=PROCESS_NAME,
)

logger = get_logger()
stdout_log_handler = create_stdout_log_handler()
file_log_handler = create_file_log_handler(
    logger_process_name=PROCESS_NAME,
    logger_file_datetime=datetime.now(timezone.utc).date(),
)
logger.addHandler(stdout_log_handler)
logger.addHandler(file_log_handler)


def main():
    logger.info(f'{PROCESS_NAME} start')

    args = parse_args()
    data_threshold_datestamp = parse_args_get_data_timestamp(args)

    start_timestamp = datetime.now(timezone.utc)
    logger.info(f'process started at {start_timestamp}')

    environment_variables = EnvironmentVariables()

    aws_access_key_id = environment_variables.get_aws_access_key_id()
    aws_secret_access_key = environment_variables.get_aws_secret_access_key()
    minio_url = environment_variables.get_minio_url()

    # TODO: move to library code
    logger.info(f'create boto3 session: minio_url={minio_url}')
    boto3_session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    postgres_connection_string = environment_variables.get_postgres_connection_string()
    logger.info(f'create database engine: postgres_host={environment_variables.get_postgres_host()}')
    postgres_engine = create_engine(postgres_connection_string)

    logger.info(f'run data initialization process')
    df = None

    with Session(postgres_engine) as session:
        row = (
            session
            .query(PPCompleteArchiveFileLog)
            .filter_by(data_threshold_datestamp=data_threshold_datestamp)
            .one_or_none()
        )

        if row is None:
            logger.warning(f'no existing row with {data_threshold_datestamp=}')
        else:

            (
                s3_bucket,
                s3_object_key,
            ) = get_s3_bucket_and_object_key_from_database(
                postgres_engine=postgres_engine,
                data_threshold_datestamp=data_threshold_datestamp,
            )

            logger.info(f's3_bucket={s3_bucket}, s3_object_key={s3_object_key}')

            boto3_client = (
                boto3_session.client(
                    's3',
                    endpoint_url=minio_url,
                    config=botocore.config.Config(signature_version='s3v4'),
                )
            )

            # TODO: change to debug
            logger.info(f'downloading data from s3 bucket')
            s3_response_object = boto3_client.get_object(Bucket=s3_bucket, Key=s3_object_key)
            pp_monthly_update_data = s3_response_object['Body'].read()

            logger.info(f'load pandas dataframe')
            df = pandas.read_csv(
                io.BytesIO(pp_monthly_update_data),
                header=None,
                # dtype=str,
                # keep_default_na=False,
            )

            datetime_now = datetime.now(timezone.utc)

            df = format_dataframe(
                df=df,
                data_timestamp=data_threshold_datestamp,
                datetime_now=datetime_now,
            )

    if not df is None:
        df.to_sql(
            schema='land_registry_2',
            name='pp_monthly_data',
            con=postgres_engine,
            if_exists='replace',
            index_label='price_paid_data_id',
            chunksize=1000,
        )

    complete_timestamp = datetime.now(timezone.utc)
    logger.info(f'process completed at {complete_timestamp}')
    duration = complete_timestamp - start_timestamp
    logger.info(f'process completed in {duration}')


def parse_args():
    parser = argparse.ArgumentParser(
        prog='database-initializer',
        description='Land Registry Data Database Initializer',
    )
    parser.add_argument('--data-threshold-datestamp', action='store', metavar='data-threshold-datestamp', dest='data_threshold_datestamp', required=True)
    args = parser.parse_args()
    return args


def parse_args_get_data_timestamp(args) -> str:

    data_threshold_datestamp = args.data_threshold_datestamp
    return data_threshold_datestamp


def format_dataframe(
    df: pandas.DataFrame,
    data_timestamp: datetime,
    datetime_now: datetime,
):

    df['data_timestamp'] = data_timestamp
    df['created_datetime'] = datetime_now

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
        'record_op',
        'data_timestamp',
        'created_datetime',
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
            'record_op': str,
            'data_timestamp': datetime,
            'created_datetime': datetime,
        }
    )

    df['transaction_date'] = pandas.to_datetime(
        arg=df['transaction_date'],
        utc=True,
        format='%Y-%m-%d %H:%M',
    )

    #df.fillna('', inplace=True)

    return df


def get_s3_bucket_and_object_key_from_database(
    postgres_engine: Engine,
    data_threshold_datestamp: datetime,
) -> tuple[str, str]:
    with Session(postgres_engine) as session:
        row = (
            session
            .query(PPCompleteArchiveFileLog)
            .filter_by(data_threshold_datestamp=data_threshold_datestamp)
            .one()
        )
        s3_bucket = row.s3_bucket
        s3_object_key = row.s3_object_key
        return (s3_bucket, s3_object_key)



if __name__ == '__main__':
    main()
