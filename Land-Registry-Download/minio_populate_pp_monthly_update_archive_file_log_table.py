
'''
This script copies files from the S3 bucket 'land-registry-monthly-data-files-txt'
to the s3 bucket 'land-registry-data-archive'.

It is used to prime the land registry data system with the historical montly
files sent from the land registry data team.

It runs over dates from 2015-01-01 to 2024-05-31.

It also creates entries in the database table PPMonthlyUpdateArchiveFileLog.
The sha256 sum is calculated for each file and added to the database table.

This script should be run first. The historical files need to be put into place
first before more recently downloaded files are added.

The historical files must already exist in S3. (In the bucket
'land-registry-monthly-data-files-txt'.) This bucket contains the files in txt
format. They were originally supplied (mostly) as zip files. Some files were
later supplied in csv or Excel formats. This was to fix issues with missing
files or corrupt data (unreadable files).

The original data files can be found in the bucket `land-registry-monthly-data-files`.
'''


import io
import hashlib
import boto3
import botocore
import pandas

from datetime import datetime
from datetime import date
from datetime import timezone
from datetime import timedelta

from sqlalchemy import Engine
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from lib_land_registry_data.lib_db import PPMonthlyUpdateArchiveFileLog
from lib_land_registry_data.lib_env import EnvironmentVariables

from lib_land_registry_data.lib_datetime import convert_to_data_publish_datestamp
from lib_land_registry_data.lib_datetime import convert_to_data_threshold_datestamp
from lib_land_registry_data.lib_datetime import convert_data_threshold_datestamp_to_data_publish_datestamp

from lib_land_registry_data.lib_dataframe import df_pp_monthly_update_columns
from lib_land_registry_data.lib_dataframe import df_pp_monthly_update_columns_no_ppd_cat


month_to_day = {
    1: 31,
    2: 28,
    3: 31,
    4: 30,
    5: 31,
    6: 30,
    7: 31,
    8: 31,
    9: 30,
    10: 31,
    11: 30,
    12: 31,
}

month_to_day_leapyear = {
    1: 31,
    2: 29,
    3: 31,
    4: 30,
    5: 31,
    6: 30,
    7: 31,
    8: 31,
    9: 30,
    10: 31,
    11: 30,
    12: 31,
}

def is_leapyear(year: int) -> bool:
    return year % 4 == 0


def boto3_get_object_and_calculate_sha256(
    boto3_client,
    bucket: str,
    object_key_txt: str
) -> None:

    s3_response_object = boto3_client.get_object(Bucket=bucket, Key=object_key_txt)
    pp_monthly_update_data = s3_response_object['Body'].read()

    print(f'reading {object_key_txt}')
    sha256sum_hex_str = hashlib.sha256(pp_monthly_update_data).hexdigest()
    print(f'sha256 of {object_key_txt}: {sha256sum_hex_str}')
    return sha256sum_hex_str


def boto3_get_object_and_calculate_data_auto_datestamp(
    boto3_client,
    bucket: str,
    object_key: str,
) -> date:

    s3_response_object = boto3_client.get_object(Bucket=bucket, Key=object_key)
    pp_monthly_update_data = s3_response_object['Body'].read()

    df = pandas.read_csv(
        io.BytesIO(pp_monthly_update_data),
        header=None,
    )
    if len(df.columns) == 15:
        df.columns = df_pp_monthly_update_columns_no_ppd_cat
    elif len(df.columns) == 16:
        df.columns = df_pp_monthly_update_columns
    else:
        raise RuntimeError(f'invalid number of columns {len(df.columns)}')
    print(df['transaction_date'].head())
    #try:
    df['transaction_date'] = pandas.to_datetime(
        arg=df['transaction_date'],
        utc=True,
        format='%Y-%m-%d %H:%M',
    )
    # except:
    #     df['transaction_date'] = pandas.to_datetime(
    #         arg=df['transaction_date'],
    #         utc=True,
    #         format='%m/%d/%Y %H:%M',
    #     )
    data_auto_datestamp = df['transaction_date'].max()
    data_auto_datestamp = (
        date(
            year=data_auto_datestamp.year,
            month=data_auto_datestamp.month,
            day=data_auto_datestamp.day,
        )
    )
    return data_auto_datestamp


def test_calculate_date_nth_working_day_of_month():
    from lib_land_registry_data.lib_datetime import calculate_date_nth_working_day_of_month

    current_date = date(
        year=2017, month=2, day=1,
    )
    print(calculate_date_nth_working_day_of_month(current_date=current_date, nth=20))
    return


def main():

    environment_variables = EnvironmentVariables()

    aws_access_key_id = environment_variables.get_aws_access_key_id()
    aws_secret_access_key = environment_variables.get_aws_secret_access_key()
    minio_url = environment_variables.get_minio_url()
    print(f'minio url: {minio_url}')

    boto3_session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    boto3_client = boto3_session.client(
        's3',
        endpoint_url=minio_url,
        config=botocore.config.Config(signature_version='s3v4'),
        use_ssl=False,
    )

    # populate_historical(
    #     environment_variables=environment_variables,
    #     boto3_client=boto3_client,
    # )
    populate_recent(
        environment_variables=environment_variables,
        boto3_client=boto3_client,
    )


def populate_historical(
    environment_variables: EnvironmentVariables,
    boto3_client,
):
    ############################################################################
    # populate historical files which have names in format
    # PPMS_update_YEAR_MONTH_DAY.txt

    pp_monthly_update_dates: list[datetime] = []

    for year in range(2015, 2024+1):
        for month in range(1, 12+1):
            if is_leapyear(year):
                day = month_to_day_leapyear[month]
            else:
                day = month_to_day[month]

            the_date = (
                datetime(
                    year=year,
                    month=month,
                    day=day,
                )
            )

            max_date = (
                datetime(
                    year=2024,
                    month=5,
                    day=31,
                )
            )

            if the_date <= max_date:
                pp_monthly_update_dates.append(the_date)

    postgres_connection_string = environment_variables.get_postgres_connection_string()
    postgres_engine = create_engine(postgres_connection_string)

    now_timestamp = datetime.now(timezone.utc)

    with Session(postgres_engine) as session:
        for target_date in pp_monthly_update_dates:
            #print(f'target_date: {target_date}')
            day = target_date.day
            month = target_date.month
            year = target_date.year

            bucket = 'land-registry-data-archive'
            object_key = f'/PPMS_update_{year}_{month}_{day}.txt'

            try:
                # boto3_get_object_and_load_pandas(
                #     boto3_client=boto3_client,
                #     bucket=bucket,
                #     object_key_txt=object_key_txt,
                # )

                sha256sum_hex_str = (
                    boto3_get_object_and_calculate_sha256(
                        boto3_client=boto3_client,
                        bucket=bucket,
                        object_key_txt=object_key,
                    )
                )

                data_auto_datestamp = (
                    boto3_get_object_and_calculate_data_auto_datestamp(
                        boto3_client=boto3_client,
                        bucket=bucket,
                        object_key=object_key,
                    )
                )

                data_threshold_datestamp = (
                    date(
                        year=int(year),
                        month=int(month),
                        day=int(day),
                    )
                )

                data_publish_datestamp = convert_data_threshold_datestamp_to_data_publish_datestamp(
                    data_threshold_datestamp=data_threshold_datestamp,
                )

                data_download_timestamp = now_timestamp

                row = PPMonthlyUpdateArchiveFileLog(
                    created_datetime=now_timestamp,
                    data_source='historical',
                    data_download_timestamp=data_download_timestamp,
                    data_publish_datestamp=data_publish_datestamp,
                    data_threshold_datestamp=data_threshold_datestamp,
                    data_auto_datestamp=data_auto_datestamp,
                    s3_bucket=bucket,
                    s3_object_key=object_key,
                    sha256sum=sha256sum_hex_str,
                )
                session.add(row)

            except botocore.exceptions.ClientError as error:
                if error.response['Error']['Code'] == 'NoSuchKey':
                    print(f'{object_key} does not exist')
                else:
                    print(f'error: {error}')
                    break

        session.commit()


def populate_recent(
    environment_variables: EnvironmentVariables,
    boto3_client,
):
    ############################################################################
    # populate downloaded files (downloaded by me) which have names in format
    # pp-monthly-updte-YEAR-MONTH-DAY.txt

    pp_monthly_update_dates: list[datetime] = []

    for year in range(2024, 2024+1):
        for month in range(6, 6+1):
            if is_leapyear(year):
                day = month_to_day_leapyear[month]
            else:
                day = month_to_day[month]

            the_date = (
                datetime(
                    year=year,
                    month=month,
                    day=day,
                )
            )

            max_date = (
                datetime(
                    year=2024,
                    month=6,
                    day=30,
                )
            )

            if the_date <= max_date:
                pp_monthly_update_dates.append(the_date)

    postgres_connection_string = environment_variables.get_postgres_connection_string()
    postgres_engine = create_engine(postgres_connection_string)

    now_timestamp = datetime.now(timezone.utc)

    with Session(postgres_engine) as session:
        for target_date in pp_monthly_update_dates:
            #print(f'target_date: {target_date}')
            day = target_date.day
            month = target_date.month
            year = target_date.year

            bucket = 'land-registry-data-archive'
            object_key = f'/pp-monthly-update-{year}-{month:02d}-{day}.txt'

            try:
                # boto3_get_object_and_load_pandas(
                #     boto3_client=boto3_client,
                #     bucket=bucket,
                #     object_key_txt=object_key_txt,
                # )

                sha256sum_hex_str = (
                    boto3_get_object_and_calculate_sha256(
                        boto3_client=boto3_client,
                        bucket=bucket,
                        object_key_txt=object_key,
                    )
                )

                data_auto_datestamp = (
                    boto3_get_object_and_calculate_data_auto_datestamp(
                        boto3_client=boto3_client,
                        bucket=bucket,
                        object_key=object_key,
                    )
                )

                data_threshold_datestamp = (
                    date(
                        year=int(year),
                        month=int(month),
                        day=int(day),
                    )
                )

                data_publish_datestamp = convert_data_threshold_datestamp_to_data_publish_datestamp(
                    data_threshold_datestamp=data_threshold_datestamp,
                )

                data_download_timestamp = now_timestamp

                row = PPMonthlyUpdateArchiveFileLog(
                    created_datetime=now_timestamp,
                    data_source='historical',
                    data_download_timestamp=data_download_timestamp,
                    data_publish_datestamp=data_publish_datestamp,
                    data_threshold_datestamp=data_threshold_datestamp,
                    data_auto_datestamp=data_auto_datestamp,
                    s3_bucket=bucket,
                    s3_object_key=object_key,
                    sha256sum=sha256sum_hex_str,
                )
                session.add(row)

            except botocore.exceptions.ClientError as error:
                if error.response['Error']['Code'] == 'NoSuchKey':
                    print(f'{object_key} does not exist')
                else:
                    print(f'error: {error}')
                    break

        session.commit()


if __name__ == '__main__':
    main()
