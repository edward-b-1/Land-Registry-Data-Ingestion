
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column

from sqlalchemy.orm import DeclarativeBase

from datetime import date
from datetime import datetime
from datetime import timedelta

from typing import Optional


class LandRegistryBase(DeclarativeBase):
    __table_args__ = {'schema': 'land_registry_2'}
    
    
# class CronTriggerLog(LandRegistryBase):
    
#     __tablename__ = 'cron_trigger_log'
    
#     cron_trigger_log_id: Mapped[int] = mapped_column(primary_key=True)
    
    
    

class PPCompleteDownloadFileLog(LandRegistryBase):
    
    __tablename__ = 'pp_complete_download_file_log'
    
    pp_complete_file_log_id: Mapped[int] = mapped_column(primary_key=True)
    
    created_datetime: Mapped[datetime]
    
    cron_target_date: Mapped[date]
    cron_target_datetime: Mapped[datetime]
    cron_trigger_datetime: Mapped[datetime]
    
    download_start_datetime: Mapped[Optional[datetime]]
    download_duration: Mapped[Optional[timedelta]]
    s3_tmp_bucket: Mapped[Optional[str]]
    s3_tmp_object_key: Mapped[Optional[str]]
    s3_upload_to_tmp_bucket_start_datetime: Mapped[Optional[datetime]]
    s3_upload_to_tmp_bucket_duration: Mapped[Optional[timedelta]]
    
    sha256sum_start_datetime: Mapped[Optional[datetime]]
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
    s3_download_from_tmp_bucket_start_datetime: Mapped[Optional[datetime]]
    s3_download_from_tmp_bucket_duration: Mapped[Optional[timedelta]]
    s3_upload_to_archive_bucket_start_datetime: Mapped[Optional[datetime]]
    s3_upload_to_archive_bucket_duration: Mapped[Optional[timedelta]]
    

class PPMonthlyUpdateDownloadFileLog(LandRegistryBase):

    __tablename__ = 'pp_monthly_update_download_file_log'

    pp_monthly_update_file_log_id: Mapped[int] = mapped_column(primary_key=True)
    
    created_datetime: Mapped[datetime]
    
    cron_target_date: Mapped[date]
    cron_target_datetime: Mapped[datetime]
    cron_trigger_datetime: Mapped[datetime]
    
    download_start_datetime: Mapped[Optional[datetime]]
    download_duration: Mapped[Optional[timedelta]]
    s3_tmp_bucket: Mapped[Optional[str]]
    s3_tmp_object_key: Mapped[Optional[str]]
    s3_upload_to_tmp_bucket_start_datetime: Mapped[Optional[datetime]]
    s3_upload_to_tmp_bucket_duration: Mapped[Optional[timedelta]]

    sha256sum_start_datetime: Mapped[Optional[datetime]]
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
    s3_download_from_tmp_bucket_start_datetime: Mapped[Optional[datetime]]
    s3_download_from_tmp_bucket_duration: Mapped[Optional[timedelta]]
    s3_upload_to_archive_bucket_start_datetime: Mapped[Optional[datetime]]
    s3_upload_to_archive_bucket_duration: Mapped[Optional[timedelta]]
    
    
class PPMonthlyUpdateArchiveFileLog(LandRegistryBase):
    
    __tablename__ = 'pp_monthly_update_archive_file_log'
    
    pp_monthly_update_archive_file_log_id: Mapped[int] = mapped_column(primary_key=True)
    
    created_datetime: Mapped[datetime]
    
    file_type: Mapped[str] # `backdated` or `current`
    file_datetime: Mapped[datetime]
    s3_bucket: Mapped[str]
    s3_object_key: Mapped[str]
    
    database_update_start_datetime: Mapped[datetime]
    database_update_duration: Mapped[timedelta]
    
    
class PPCompleteArchiveFileLog(LandRegistryBase):
    
    __tablename__ = 'pp_complete_archive_file_log'
    
    pp_complete_archive_file_log_id: Mapped[int] = mapped_column(primary_key=True)
    
    created_datetime: Mapped[datetime]
    
    file_type: Mapped[str] # `backdated` or `current`
    file_datetime: Mapped[datetime]
    s3_bucket: Mapped[str]
    s3_object_key: Mapped[str]
    
    database_upload_start_datetime: Mapped[datetime]
    database_upload_duration: Mapped[timedelta]
    
    
class PPDataConsistencyCheckLog(LandRegistryBase):
    
    __tablename__ = 'pp_data_consistency_check_log'
    
    pp_data_consistency_check_log_id: Mapped[int] = mapped_column(primary_key=True)
    
    created_datetime: Mapped[datetime]
    
    consistency_check_start_datetime: Mapped[datetime]
    consistency_check_duration: Mapped[timedelta]
    consistency_check_result: Mapped[str]
    
    row_count_pp_complete_only: Mapped[int]
    row_count_pp_monthly_update_only: Mapped[int]
    
    
class PPDataReconciliationLog(LandRegistryBase):
    
    __tablename__ = 'pp_data_reconciliation_log'
    
    pp_data_reconciliation_log_id: Mapped[int] = mapped_column(primary_key=True)
    
    created_datetime: Mapped[datetime]
    
    reconciliation_start_datetime: Mapped[datetime]
    reconciliation_duration: Mapped[timedelta]
    
    # TODO: other fields...    

# class PricePaidData(LandRegistryBase):

#     __tablename__ = 'price_paid_data'

#     price_paid_data_id: Mapped[int] = mapped_column(primary_key=True)
#     transaction_unique_id: Mapped[str]
#     price: Mapped[int]
#     transaction_date: Mapped[datetime]
#     postcode: Mapped[str]
#     property_type: Mapped[str]
#     new_tag: Mapped[str]
#     lease: Mapped[str]
#     primary_address_object_name: Mapped[str]
#     secondary_address_object_name: Mapped[str]
#     street: Mapped[str]
#     locality: Mapped[str]
#     town_city: Mapped[str]
#     district: Mapped[str]
#     county: Mapped[str]
#     ppd_cat: Mapped[str]
#     #record_status: Mapped[str]
#     file_date: Mapped[date]
#     is_deleted: Mapped[str]
#     created_datetime: Mapped[datetime]
#     updated_datetime: Mapped[datetime]
#     deleted_datetime: Mapped[datetime]


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

