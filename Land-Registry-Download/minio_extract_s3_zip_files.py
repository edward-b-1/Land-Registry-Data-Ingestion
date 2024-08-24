'''
This script takes the files from the S3 bucket 'land-registry-monthly-data-files'
and copies them to 'land-registry-monthly-data-files-txt'.

The original files are (mostly) in zip file format. This script will extract the
zip files and copy the inner txt file to the destination bucket.

There are some missing zip files. These are reported.

One zip file contains two txt files. One of these is ignored, because it is for
the wrong month, and the correct month txt file is uploaded.

The files are renamed so that the date is in YYYY-MM-DD format rather than
DD-MM-YYYY format. The original files use month name strings, the renamed files
use month numbers.
'''


import io
import hashlib
import boto3
import botocore

from zipfile import ZipFile

from datetime import datetime


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

month_string_to_integer = {
    'jan': 1,
    'feb': 2,
    'mar': 3,
    'apr': 4,
    'may': 5,
    'jun': 6,
    'jul': 7,
    'aug': 8,
    'sep': 9,
    'oct': 10,
    'nov': 11,
    'dec': 12,
}

month_integer_to_string = {
    1: 'jan',
    2: 'feb',
    3: 'mar',
    4: 'apr',
    5: 'may',
    6: 'jun',
    7: 'jul',
    8: 'aug',
    9: 'sep',
    10: 'oct',
    11: 'nov',
    12: 'dec',
}

def is_leapyear(year: int) -> bool:
    return year % 4 == 0


def boto3_get_object_unzip_and_upload(
    boto3_client,
    bucket: str,
    object_key_zip: str
) -> None:

    s3_response_object = boto3_client.get_object(Bucket=bucket, Key=object_key_zip)
    pp_monthly_update_zip_file_data = s3_response_object['Body'].read()

    year = object_key_zip.split('_')[4]
    month_str = object_key_zip.split('_')[3]
    day = object_key_zip.split('_')[2]
    month = month_string_to_integer[month_str]

    with ZipFile(io.BytesIO(pp_monthly_update_zip_file_data), 'r') as zipfile:
        filename_list = zipfile.namelist()

        assert len(filename_list) > 0, 'zero files'
        if len(filename_list) > 1:
            print(f'zipfile {object_key_zip} contains {len(filename_list)} files')
            for filename in filename_list:
                print(f'contains: {filename}')

        target_filename = f'PPMS_update_{day}_{month_str}_{year}_ew.txt'

        filename_list = (
            list(
                filter(
                    lambda filename: filename == target_filename,
                    filename_list,
                )
            )
        )

        assert len(filename_list) == 1, f'could not get target filename {target_filename} from file {object_key_zip}'

        for filename in filename_list:
            #print(f'filename: {filename}')
            with zipfile.open(filename) as ifile:
                destination_bucket = 'land-registry-monthly-data-files-txt'
                #object_key_txt = object_key_zip.replace('.zip', '.txt')

                object_key_txt = f'PPMS_update_{year}_{month}_{day}.txt'
                boto3_client.put_object(Bucket=destination_bucket, Key=object_key_txt, Body=ifile.read())


def boto3_get_object_unzip_and_calculate_sha256(
    boto3_client,
    bucket: str,
    object_key_zip: str
) -> str:

    s3_response_object = boto3_client.get_object(Bucket=bucket, Key=object_key_zip)
    pp_monthly_update_zip_file_data = s3_response_object['Body'].read()

    with ZipFile(io.Bytes(pp_monthly_update_zip_file_data), 'r') as zipfile:
        assert len(zipfile.namelist()) == 1, f'zipfile {object_key_zip} contains {len(zipfile.namelist())} files'

        for filename in zipfile.namelist():
            print(f'filename: {filename}')
            with zipfile.open(filename) as ifile:
                sha256sum_hex_str = hashlib.sha256(ifile.read()).hexdigest()

                print(f'sha256: {sha256sum_hex_str}')
                return sha256sum_hex_str


def boto3_get_object_and_calculate_sha256(
    boto3_client,
    bucket: str,
    object_key: str
) -> str:
    s3_response_object = boto3_client.get_object(Bucket=bucket, Key=object_key)
    pp_monthly_update_zip_file_data = s3_response_object['Body'].read()

    sha256sum_hex_str = hashlib.sha256(pp_monthly_update_zip_file_data).hexdigest()

    print(f'sha256: {sha256sum_hex_str}')
    return sha256sum_hex_str



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

    for target_date in pp_monthly_update_dates:
        #print(f'target_date: {target_date}')
        day = target_date.day
        month = target_date.month
        year = target_date.year
        month_str = month_integer_to_string[month]

        bucket = 'land-registry-monthly-data-files'
        object_key_zip = f'/original-data/PPMS_update_{day}_{month_str}_{year}_ew.zip'
        # object_key_csv = f'/original-data/PPMS_update_{day}_{month_str}_{year}_ew.csv'
        # object_key_txt = f'/original-data/PPMS_update_{day}_{month_str}_{year}_ew.txt'

        #print(f'object_key: {object_key_zip}')

        try:
            boto3_get_object_unzip_and_upload(
                boto3_client=boto3_client,
                bucket=bucket,
                object_key_zip=object_key_zip,
            )

        except botocore.exceptions.ClientError as error:
            if error.response['Error']['Code'] == 'NoSuchKey':
                print(f'{object_key_zip} does not exist')
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
