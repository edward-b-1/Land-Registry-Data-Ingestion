'''
This script copies pp-monthly-update-XXX.txt files from a directory on the
development server `devbox2` to the S3 bucket 'land-registry-monthly-data-files-txt'.

Historical files were supplied by the land registry data team up to the date
2024-05-31.

My first version of the land registry data downloaded has been downloading files
since approximatly March 2024. Hence I have some files which overlap with the
files sent by the land registry data team. I have some more recent files which
the land registry data team did not send to me.

This script takes all the files which I have available to me from the devbox2
directory, calculates the sha256 sum of each of them, and uses this to detect
whether there is already a copy of the file on S3. If the file already exists,
it is ignored. Otherwise, it is uploaded to the bucket 'land-registry-data-archive'
and a row is added to the database table PPMonthlyUpdateArchiveFileLog.

Files downloaded from the URL endpoints changed on the 20th working day of each
month, and these files contain transactions up to the last (probably working)
day of the previous month. Hence a file with a date stamp of 2024-07-27.txt
contains data up to 2024-06-30.txt.

This script should be run every time there is new data available to be uploaded.

At some point this script will become obsolete, because the new data injection
system will move files from the s3 bucket 'land-registry-data-tmp' to the bucket
'land-registry-data-archive' directly. When this time comes the old data
injestion system will be turned off.
'''


import os
import hashlib
import boto3
import botocore
import pandas

from datetime import datetime
from datetime import date
from datetime import timezone

from sqlalchemy import Engine
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from lib_land_registry_data.lib_dataframe import df_pp_monthly_update_columns

from lib_land_registry_data.lib_db import PPMonthlyUpdateArchiveFileLog
from lib_land_registry_data.lib_env import EnvironmentVariables

from lib_land_registry_data.lib_datetime import convert_to_data_publish_datestamp
from lib_land_registry_data.lib_datetime import convert_to_data_threshold_datestamp

from lib_land_registry_data.lib_dataframe import df_pp_monthly_update_columns
from lib_land_registry_data.lib_dataframe import df_pp_monthly_update_columns_no_ppd_cat


def is_leapyear(year: int) -> bool:
    return year % 4 == 0

days_in_month_non_leapyear = {
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

days_in_month_leapyear = {
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


def fast_scandir_2(dirname: str) -> list[str]:
    files = []
    for f in os.scandir(dirname):
        if f.is_dir():
            files.extend(fast_scandir_2(f.path))
        if f.is_file():
            files.append(f.path)
    files.sort()
    return files


def calculate_sha256sum(
    file_path: str,
) -> str|None:
    with open(file_path, 'rb') as ifile:
        sha256sum = hashlib.file_digest(ifile, 'sha256').hexdigest()
        return sha256sum
    return None


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

    postgres_connection_string = environment_variables.get_postgres_connection_string()
    postgres_engine = create_engine(postgres_connection_string)

    now_timestamp = datetime.now(timezone.utc)

    target_directory = f'/data-land-registry/pp-monthly-update-rename'

    files = fast_scandir_2(target_directory)

    files = (
        list(
            filter(
                lambda file: not 'copy-of-new-files' in file,
                files,
            )
        )
    )

    for file in files:
        #print(file)

        sha256 = calculate_sha256sum(file_path=file)

        with Session(postgres_engine) as session:
            rows = (
                session
                .query(PPMonthlyUpdateArchiveFileLog)
                .filter_by(sha256sum=sha256)
                .all()
            )

            if len(rows) < 1:
                print(f'no existing rows with sha256 {sha256}, file {file} does not exist in database')

                file_name = file.rsplit('/', maxsplit=1)[1]
                file_name_no_ext = file_name.rsplit('.', maxsplit=1)[0]

                year = file_name_no_ext.split('-')[3]
                month = file_name_no_ext.split('-')[4]
                day = file_name_no_ext.split('-')[5]

                print(f'file is stamped with date: {year}-{month}-{day}')

                data_download_date = (
                    date(
                        year=year,
                        month=month,
                        day=day,
                    )
                )

                # # wind back to last day of last month
                # month = month - 1 if month > 1 else 12
                # year = year if month > 1 else year - 1
                # if is_leapyear(year):
                #     day = days_in_month_leapyear[month]
                # else:
                #     day = days_in_month_non_leapyear[month]

                # print(f'data threshold date: {year}-{month}-{day}')

                # rename and send to s3
                renamed_filename = f'pp-monthly-update-{year}-{month}-{day}.txt'
                print(f'renamed: {renamed_filename}')
                data_download_timestamp = (
                    datetime(
                        year=int(year),
                        month=int(month),
                        day=int(day),
                        tzinfo=timezone.utc,
                    )
                )

                data_publish_datestamp = convert_to_data_publish_datestamp(data_download_timestamp)
                data_threshold_datestamp = convert_to_data_threshold_datestamp(data_download_timestamp)

                df = pandas.read_csv(
                    file,
                    header=None,
                )
                # TODO: some files have 15 columns not 16
                if len(df.columns) == 15:
                    df.columns = df_pp_monthly_update_columns_no_ppd_cat
                elif len(df.columns) == 16:
                    df.columns = df_pp_monthly_update_columns
                else:
                    raise RuntimeError(f'invalid number of columns {len(df.columns)}')
                df['transaction_date'] = pandas.to_datetime(
                    arg=df['transaction_date'],
                    utc=True,
                    format='%Y-%m-%d %H:%M',
                )
                data_auto_datestamp = df['transaction_date'].max()
                data_auto_datestamp = (
                    date(
                        year=data_auto_datestamp.year,
                        month=data_auto_datestamp.month,
                        day=data_auto_datestamp.day,
                    )
                )

                boto3_client.upload_file(file, 'land-registry-data-archive', renamed_filename)

                row = PPMonthlyUpdateArchiveFileLog(
                    created_datetime=now_timestamp,
                    data_source='current',
                    data_download_timestamp=data_download_timestamp,
                    data_publish_datestamp=data_publish_datestamp,
                    data_threshold_datestamp=data_threshold_datestamp,
                    data_auto_datestamp=data_auto_datestamp,
                    s3_bucket='land-registry-data-archive',
                    s3_object_key=renamed_filename,
                    sha256sum=sha256,
                )
                session.add(row)
                session.commit()

            elif len(rows) > 1:
                print(f'error: duplicate rows')
            else:
                s3_object_key = rows[0].s3_object_key
                print(f'1 existing row with sha256 {sha256}, file {file} matches {s3_object_key}')


if __name__ == '__main__':
    main()

