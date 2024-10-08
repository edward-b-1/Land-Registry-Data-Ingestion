
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column

from sqlalchemy.orm import DeclarativeBase

from datetime import date
from datetime import datetime
from datetime import timedelta

from typing import Optional


class LandRegistryBase(DeclarativeBase):
    __table_args__ = {'schema': 'land_registry_2'}


class PPCompleteDownloadFileLog(LandRegistryBase):

    __tablename__ = 'pp_complete_download_file_log'

    pp_complete_download_file_log_id: Mapped[int] = mapped_column(primary_key=True)

    created_datetime: Mapped[datetime]

    cron_target_date: Mapped[date]
    cron_target_datetime: Mapped[datetime]
    cron_trigger_datetime: Mapped[datetime]

    download_start_timestamp: Mapped[Optional[datetime]] # aka data_download_timestamp
    download_duration: Mapped[Optional[timedelta]]
    data_publish_datestamp: Mapped[Optional[date]] # 20th day of current month or last month
    data_threshold_datestamp: Mapped[Optional[date]] # last day of last month - careful, rolls over on 20th!
    data_auto_datestamp: Mapped[Optional[date]] # detected from file contents
    s3_tmp_bucket: Mapped[Optional[str]]
    s3_tmp_object_key: Mapped[Optional[str]]
    s3_upload_to_tmp_bucket_start_timestamp: Mapped[Optional[datetime]]
    s3_upload_to_tmp_bucket_duration: Mapped[Optional[timedelta]]

    sha256sum_start_timestamp: Mapped[Optional[datetime]]
    sha256sum_duration: Mapped[Optional[timedelta]]
    sha256sum: Mapped[Optional[str]]

    data_decision_datetime: Mapped[Optional[datetime]]
    data_decision: Mapped[Optional[str]] # garbage_collect or archive

    gc_datetime: Mapped[Optional[datetime]]
    gc_action_taken: Mapped[Optional[str]]

    s3_archive_datetime: Mapped[Optional[datetime]]
    s3_archive_action_taken: Mapped[Optional[str]]
    s3_archive_bucket: Mapped[Optional[str]]
    s3_archive_object_key: Mapped[Optional[str]]
    s3_copy_start_timestamp: Mapped[Optional[datetime]]
    s3_copy_duration: Mapped[Optional[timedelta]]


class PPMonthlyUpdateDownloadFileLog(LandRegistryBase):

    __tablename__ = 'pp_monthly_update_download_file_log'

    pp_monthly_update_download_file_log_id: Mapped[int] = mapped_column(primary_key=True)

    created_datetime: Mapped[datetime]

    cron_target_date: Mapped[date]
    cron_target_datetime: Mapped[datetime]
    cron_trigger_datetime: Mapped[datetime]

    download_start_timestamp: Mapped[Optional[datetime]] # aka data_download_timestamp
    download_duration: Mapped[Optional[timedelta]]
    data_publish_datestamp: Mapped[Optional[date]] # 20th day of current month or last month
    data_threshold_datestamp: Mapped[Optional[date]] # last day of last month - careful, rolls over on 20th!
    data_auto_datestamp: Mapped[Optional[date]] # detected from file contents
    s3_tmp_bucket: Mapped[Optional[str]]
    s3_tmp_object_key: Mapped[Optional[str]]
    s3_upload_to_tmp_bucket_start_timestamp: Mapped[Optional[datetime]]
    s3_upload_to_tmp_bucket_duration: Mapped[Optional[timedelta]]

    sha256sum_start_timestamp: Mapped[Optional[datetime]]
    sha256sum_duration: Mapped[Optional[timedelta]]
    sha256sum: Mapped[Optional[str]]

    data_decision_datetime: Mapped[Optional[datetime]]
    data_decision: Mapped[Optional[str]] # garbage_collect or archive

    gc_datetime: Mapped[Optional[datetime]]
    gc_action_taken: Mapped[Optional[str]]

    s3_archive_datetime: Mapped[Optional[datetime]]
    s3_archive_action_taken: Mapped[Optional[str]]
    s3_archive_bucket: Mapped[Optional[str]]
    s3_archive_object_key: Mapped[Optional[str]]
    s3_copy_start_timestamp: Mapped[Optional[datetime]]
    s3_copy_duration: Mapped[Optional[timedelta]]


# NOTE:
# historical data files should exist in S3 bucket
# script should be run to calculate shasums of existing files
# and add rows to this db table
# the existing files should be "queried" using known names
# for example, we know the name should be pp-monthly-update-DATE.txt
# and all the dates are known in advance.
# if a file does not exist, this is an error
# the column names are known in advance, there are two column name
# schemas, one with and one without the ppdcat column
class PPMonthlyUpdateArchiveFileLog(LandRegistryBase):

    __tablename__ = 'pp_monthly_update_archive_file_log'

    pp_monthly_update_archive_file_log_id: Mapped[int] = mapped_column(primary_key=True)

    created_datetime: Mapped[datetime]

    data_source: Mapped[str] # `historical` or `current`
    data_download_timestamp: Mapped[datetime]
    data_publish_datestamp: Mapped[Optional[date]] # 20th day of current month or last month
    data_threshold_datestamp: Mapped[Optional[date]] # last day of last month - careful, rolls over on 20th!
    data_auto_datestamp: Mapped[Optional[date]] # detected from file contents
    s3_bucket: Mapped[str]
    s3_object_key: Mapped[str]
    sha256sum: Mapped[str]
    # TODO: need a row for "what date the file is for: year-month-day"

    database_update_start_timestamp: Mapped[Optional[datetime]]
    database_update_duration: Mapped[Optional[timedelta]]


class PPCompleteArchiveFileLog(LandRegistryBase):

    __tablename__ = 'pp_complete_archive_file_log'

    pp_complete_archive_file_log_id: Mapped[int] = mapped_column(primary_key=True)

    created_datetime: Mapped[datetime]

    data_source: Mapped[str] # `historical` or `current`
    data_download_timestamp: Mapped[datetime]
    data_publish_datestamp: Mapped[Optional[date]] # 20th day of current month or last month
    data_threshold_datestamp: Mapped[Optional[date]] # last day of last month - careful, rolls over on 20th!
    data_auto_datestamp: Mapped[Optional[date]] # detected from file contents
    s3_bucket: Mapped[str]
    s3_object_key: Mapped[str]
    sha256sum: Mapped[str]
    # TODO: need a row for "what date the file is for: year-month-day"

    database_upload_start_timestamp: Mapped[Optional[datetime]]
    database_upload_duration: Mapped[Optional[timedelta]]


class PPMonthlyDataOperationLog(LandRegistryBase):
    '''
    This table is used to control the data flow for initialization of the
    PPMonthlyData table.

    The first row created will record that a copy of pp-complete.txt has been
    used to initialize the data.

    Following this, all future operations will be monthly update operations.
    A copy of pp-monthly-update.txt will be used to update the data.

    The data_publish_timestamp is used to check if the data for
    pp-monthly-update.txt is already present because it has been uploaded as
    part of the initialization step using pp-complete.txt.
    '''

    __tablename__ = 'pp_monthly_data_operation_log'

    pp_monthly_data_operation_log_id: Mapped[int] = mapped_column(primary_key=True)

    created_datetime: Mapped[datetime]

    operation_category: Mapped[str] # initialize | monthly-update
    data_source: Mapped[str] # pp-complete | pp-monthly-update
    data_publish_datestamp: Mapped[date]
    s3_bucket: Mapped[str]
    s3_object_key: Mapped[str]
    sha256sum: Mapped[str]


class PPDataConsistencyCheckLog(LandRegistryBase):

    __tablename__ = 'pp_data_consistency_check_log'

    pp_data_consistency_check_log_id: Mapped[int] = mapped_column(primary_key=True)

    created_datetime: Mapped[datetime]

    consistency_check_start_timestamp: Mapped[datetime]
    consistency_check_duration: Mapped[timedelta]
    consistency_check_result: Mapped[str]

    row_count_pp_complete_only: Mapped[int]
    row_count_pp_monthly_update_only: Mapped[int]


class PPDataReconciliationLog(LandRegistryBase):

    __tablename__ = 'pp_data_reconciliation_log'

    pp_data_reconciliation_log_id: Mapped[int] = mapped_column(primary_key=True)

    created_datetime: Mapped[datetime]

    reconciliation_start_timestamp: Mapped[datetime]
    reconciliation_duration: Mapped[timedelta]

    # TODO: other fields...


class PPCompleteData(LandRegistryBase):

    __tablename__ = 'pp_complete_data'

    price_paid_data_id: Mapped[int] = mapped_column(primary_key=True)
    transaction_unique_id: Mapped[str]
    price: Mapped[int]
    transaction_date: Mapped[datetime]
    postcode: Mapped[str]
    property_type: Mapped[str]
    new_tag: Mapped[str]
    lease: Mapped[str]
    primary_address_object_name: Mapped[str]
    secondary_address_object_name: Mapped[str]
    street: Mapped[str]
    locality: Mapped[str]
    town_city: Mapped[str]
    district: Mapped[str]
    county: Mapped[str]
    ppd_cat: Mapped[Optional[str]]
    record_op: Mapped[str]
    data_datestamp: Mapped[date]
    data_publish_datestamp: Mapped[date]
    data_threshold_datestamp: Mapped[date]
    data_auto_datestamp: Mapped[date]
    created_datetime: Mapped[datetime] # Time row with UUID first created


class PPMonthlyData(LandRegistryBase):

    __tablename__ = 'pp_monthly_data'

    price_paid_data_id: Mapped[int] = mapped_column(primary_key=True)
    transaction_unique_id: Mapped[str]
    price: Mapped[int]
    transaction_date: Mapped[datetime]
    postcode: Mapped[str]
    property_type: Mapped[str]
    new_tag: Mapped[str]
    lease: Mapped[str]
    primary_address_object_name: Mapped[str]
    secondary_address_object_name: Mapped[str]
    street: Mapped[str]
    locality: Mapped[str]
    town_city: Mapped[str]
    district: Mapped[str]
    county: Mapped[str]
    ppd_cat: Mapped[Optional[str]]
    record_op: Mapped[str]
    data_datestamp: Mapped[date]
    data_publish_datestamp: Mapped[date]
    data_threshold_datestamp: Mapped[date]
    data_auto_datestamp: Mapped[date]
    insert_op_count: Mapped[int] # Insert op count
    update_op_count: Mapped[int] # Update op count
    delete_op_count: Mapped[int] # Delete op count
    created_datetime: Mapped[datetime] # Time row with UUID first created
    inserted_datetime: Mapped[datetime] # Last time row with UUID insert op
    updated_datetime: Mapped[Optional[datetime]] # Last time row with UUID updated op
    deleted_datetime: Mapped[Optional[datetime]] # Last time row with UUID deleted op
    is_deleted: Mapped[bool]


# class PricePaidDataLog(LandRegistryBase):

#     __tablename__ = 'price_paid_data_log'

#     price_paid_data_log_id: Mapped[int] = mapped_column(primary_key=True)
#     log_timestamp: Mapped[datetime]
#     query_row_count_total: Mapped[int]
#     query_row_count_inserted: Mapped[int]
#     query_row_count_updated: Mapped[int]
#     query_row_count_deleted: Mapped[int]
#     row_count_before: Mapped[int]
#     row_count_after: Mapped[int]

#     # download_data_row_count: Mapped[int]        # The number of data entries in the downloaded dataset
#     # download_data_rows_removed: Mapped[int]     # The number of data entries removed because they already exist in the database
#     # download_data_rows_remaining: Mapped[int]   # The number of rows remaining after row removal
#     # row_count_before: Mapped[int]               # The number of rows in the database before
#     # rows_added: Mapped[int]                     # The number of rows added
#     # rows_updated: Mapped[int]                   # The number of rows updated (data changed, not new row)
#     # row_count_after: Mapped[int]                # The number of rows in the database after


# # NOTE: there might be more than one entry in the database for each file as this
# # table allows files to be processed more than once
# class PricePaidDataMonthlyUpdateDatabaseUpdaterOperationLog(LandRegistryBase):

#     __tablename__ = 'price_paid_data_monthly_update_database_updater_operation_log'

#     price_paid_data_monthly_update_operation_log_id: Mapped[int] = mapped_column(primary_key=True)
#     filename: Mapped[str]
#     processed_datetime: Mapped[datetime]
#     input_file_row_count: Mapped[int]
#     input_file_row_count_insert: Mapped[int]
#     input_file_row_count_update: Mapped[int]
#     input_file_row_count_delete: Mapped[int]
#     operation_count_insert: Mapped[int]
#     operation_count_update: Mapped[int]
#     operation_count_delete: Mapped[int]
#     operation_count_ignored: Mapped[int]
#     operation_count_insert_insert: Mapped[int]
#     operation_count_insert_update: Mapped[int]
#     operation_count_insert_ignore: Mapped[int]
#     operation_count_update_update: Mapped[int]
#     operation_count_update_ignore: Mapped[int]
#     operation_count_update_insert: Mapped[int]
#     operation_count_delete_delete: Mapped[int]
#     operation_count_delete_change_delete: Mapped[int]
#     operation_count_delete_ignore: Mapped[int]

