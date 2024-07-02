
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column

from sqlalchemy.orm import DeclarativeBase

from datetime import datetime

from typing import Optional


class LandRegistryBase(DeclarativeBase):
    __table_args__ = {'schema': 'land_registry'}


class PricePaidData(LandRegistryBase):

    __tablename__ = 'price_paid_data'

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
    ppd_cat: Mapped[str]
    record_status: Mapped[str]
    is_deleted: Mapped[str]
    created_datetime: Mapped[datetime]
    updated_datetime: Mapped[Optional[datetime]]
    deleted_datetime: Mapped[Optional[datetime]]
    created_datetime_original: Mapped[Optional[datetime]]


class PricePaidDataLog(LandRegistryBase):

    __tablename__ = 'price_paid_data_log'

    price_paid_data_log_id: Mapped[int] = mapped_column(primary_key=True)
    log_timestamp: Mapped[datetime]
    query_row_count_total: Mapped[int]
    query_row_count_inserted: Mapped[int]
    query_row_count_updated: Mapped[int]
    query_row_count_deleted: Mapped[int]
    row_count_before: Mapped[int]
    row_count_after: Mapped[int]

    # download_data_row_count: Mapped[int]        # The number of data entries in the downloaded dataset
    # download_data_rows_removed: Mapped[int]     # The number of data entries removed because they already exist in the database
    # download_data_rows_remaining: Mapped[int]   # The number of rows remaining after row removal
    # row_count_before: Mapped[int]               # The number of rows in the database before
    # rows_added: Mapped[int]                     # The number of rows added
    # rows_updated: Mapped[int]                   # The number of rows updated (data changed, not new row)
    # row_count_after: Mapped[int]                # The number of rows in the database after


class PricePaidDataMonthlyUpdateFileLog(LandRegistryBase):

    __tablename__ = 'price_paid_data_monthly_update_file_log'

    price_paid_data_monthly_update_file_log_id: Mapped[int] = mapped_column(primary_key=True)
    filename: Mapped[str]
    sha256sum: Mapped[str]
    created_datetime: Mapped[datetime]
    processed_datetime: Mapped[datetime]
    process_decision: Mapped[str]
    uploaded_datetime: Mapped[datetime]
    deleted_datetime: Mapped[datetime]


# NOTE: there might be more than one entry in the database for each file as this
# table allows files to be processed more than once
class PricePaidDataMonthlyUpdateDatabaseUpdaterOperationLog(LandRegistryBase):

    __tablename__ = 'price_paid_data_monthly_update_database_updater_operation_log'

    price_paid_data_monthly_update_operation_log_id: Mapped[int] = mapped_column(primary_key=True)
    filename: Mapped[str]
    processed_datetime: Mapped[datetime]
    input_file_row_count: Mapped[int]
    input_file_row_count_insert: Mapped[int]
    input_file_row_count_update: Mapped[int]
    input_file_row_count_delete: Mapped[int]
    operation_count_insert: Mapped[int]
    operation_count_update: Mapped[int]
    operation_count_delete: Mapped[int]
    operation_count_ignored: Mapped[int]
    operation_count_insert_insert: Mapped[int]
    operation_count_insert_update: Mapped[int]
    operation_count_insert_ignore: Mapped[int]
    operation_count_update_update: Mapped[int]
    operation_count_update_ignore: Mapped[int]
    operation_count_update_insert: Mapped[int]
    operation_count_delete_delete: Mapped[int]
    operation_count_delete_change_delete: Mapped[int]
    operation_count_delete_ignore: Mapped[int]
