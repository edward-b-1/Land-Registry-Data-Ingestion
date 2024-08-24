
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
from datetime import timezone

from sqlalchemy import Engine
from sqlalchemy import create_engine
from sqlalchemy.orm import Session


from lib_land_registry_data.lib_db import PPMonthlyUpdateArchiveFileLog
from lib_land_registry_data.lib_env import EnvironmentVariables


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


def boto3_get_object_and_load_pandas(
    boto3_client,
    bucket: str,
    object_key_txt: str
) -> None:

    s3_response_object = boto3_client.get_object(Bucket=bucket, Key=object_key_txt)
    pp_monthly_update_zip_file_data = s3_response_object['Body'].read()

    print(f'reading {object_key_txt}')

    try:
        df = pandas.read_csv(
            io.BytesIO(pp_monthly_update_zip_file_data),
            header=None,
        )
        print(df.head())

    except Exception as error:
        print(f'can\'t read {object_key_txt}, saving to disk for inspection')
        print(f'{error}')
        object_key_txt_tmp = object_key_txt.replace('/', '')
        object_key_txt_tmp = f'{object_key_txt_tmp}.tmp'
        boto3_client.download_file(bucket, object_key_txt, object_key_txt_tmp)


def boto3_get_object_and_calculate_sha256(
    boto3_client,
    bucket: str,
    object_key_txt: str
) -> None:

    s3_response_object = boto3_client.get_object(Bucket=bucket, Key=object_key_txt)
    pp_monthly_update_zip_file_data = s3_response_object['Body'].read()

    print(f'reading {object_key_txt}')

    try:
        sha256sum_hex_str = hashlib.sha256(pp_monthly_update_zip_file_data).hexdigest()
        print(f'sha256 of {object_key_txt}: {sha256sum_hex_str}')
        return sha256sum_hex_str

    except Exception as error:
        print(f'can\'t read {object_key_txt}, saving to disk for inspection')
        print(f'{error}')
        object_key_txt_tmp = object_key_txt.replace('/', '')
        object_key_txt_tmp = f'{object_key_txt_tmp}.tmp'
        boto3_client.download_file(bucket, object_key_txt, object_key_txt_tmp)


def boto3_get_object_unzip_and_calculate_sha256(
    boto3_client,
    bucket: str,
    object_key_zip: str
) -> str:

    s3_response_object = boto3_client.get_object(Bucket=bucket, Key=object_key_zip)
    pp_monthly_update_zip_file_data = s3_response_object['Body'].read()

    sha256sum_hex_str = hashlib.sha256(pp_monthly_update_zip_file_data).hexdigest()

    print(f'sha256: {sha256sum_hex_str}')
    return sha256sum_hex_str


# def boto3_get_object_and_calculate_sha256(
#     boto3_client,
#     bucket: str,
#     object_key: str
# ) -> str:
#     s3_response_object = boto3_client.get_object(Bucket=bucket, Key=object_key)
#     pp_monthly_update_zip_file_data = s3_response_object['Body'].read()

#     sha256sum_hex_str = hashlib.sha256(pp_monthly_update_zip_file_data).hexdigest()

#     print(f'sha256: {sha256sum_hex_str}')
#     return sha256sum_hex_str


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

    pp_monthly_update_dates: list[datetime] = []

    for year in range(2015, 2024+1):
        for month in range(1, 12+1):
            if is_leapyear(year):
                day = month_to_day_leapyear[month]
            else:
                day = month_to_day[month]

            date = (
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

            if date <= max_date:
                pp_monthly_update_dates.append(date)

    postgres_connection_string = environment_variables.get_postgres_connection_string()
    postgres_engine = create_engine(postgres_connection_string)

    now_timestamp = datetime.now(timezone.utc)

    for target_date in pp_monthly_update_dates:
        #print(f'target_date: {target_date}')
        day = target_date.day
        month = target_date.month
        year = target_date.year

        bucket = 'land-registry-monthly-data-files-txt'
        object_key_txt = f'/PPMS_update_{year}_{month}_{day}.txt'
        # object_key_csv = f'/original-data/PPMS_update_{day}_{month_str}_{year}_ew.csv'
        # object_key_txt = f'/original-data/PPMS_update_{day}_{month_str}_{year}_ew.txt'

        #print(f'object_key: {object_key_zip}')

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
                    object_key_txt=object_key_txt,
                )
            )

            bucket_destination = 'land-registry-data-archive'
            object_key = f'PPMS_update_{year}_{month}_{day}.txt'

            boto3_client.copy(
                CopySource={
                    'Bucket': bucket,
                    'Key': object_key_txt.replace('/', ''),
                },
                Bucket=bucket_destination,
                Key=object_key,
            )

            with Session(postgres_engine) as session:
                data_timestamp = (
                    datetime(
                        year=year,
                        month=month,
                        day=day,
                        tzinfo=timezone.utc,
                    )
                )

                row = PPMonthlyUpdateArchiveFileLog(
                    created_datetime=now_timestamp,
                    data_source='historical',
                    data_timestamp=data_timestamp,
                    s3_bucket=bucket_destination,
                    s3_object_key=object_key,
                    sha256sum=sha256sum_hex_str,
                )
                session.add(row)
                session.commit()

        except botocore.exceptions.ClientError as error:
            if error.response['Error']['Code'] == 'NoSuchKey':
                print(f'{object_key_txt} does not exist')
                # print(f'trying {object_key_csv}')

                # try:
                #     boto3_get_object_and_calculate_sha256()

                # except botocore.exceptions.ClientError as error:
                #     if error.response['Error']['Code'] == 'NoSuchKey':
                #         print(f'{object_key_csv} does not exist')
                #         print(f'trying {object_key_txt}')
            else:
                print(f'error: {error}')
                break


if __name__ == '__main__':
    main()
