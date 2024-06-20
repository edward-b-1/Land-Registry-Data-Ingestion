create table land_registry.price_paid_data_monthly_update_database_updater_operation_log (
	price_paid_data_monthly_update_operation_log_id bigserial not null primary key,
	filename varchar not null,
    processed_datetime timestamptz not null,
    input_file_row_count int8 not null,
    input_file_row_count_insert int8 not null,
    input_file_row_count_update int8 not null,
    input_file_row_count_delete int8 not null,
    operation_count_insert int8 not null,
    operation_count_update int8 not null,
    operation_count_delete int8 not null,
    operation_count_ignored int8 not null,
    operation_count_insert_insert int8 not null,
    operation_count_insert_update int8 not null,
    operation_count_insert_ignore int8 not null,
    operation_count_update_update int8 not null,
    operation_count_update_ignore int8 not null,
    operation_count_update_insert int8 not null,
    operation_count_delete_delete int8 not null,
    operation_count_delete_change_delete int8 not null,
    operation_count_delete_ignore int8 not null
);