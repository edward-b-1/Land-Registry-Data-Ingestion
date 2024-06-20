
create schema land_registry;

create table land_registry.price_paid_data (
	price_paid_data_id bigserial not null primary key,
	transaction_unique_id varchar not null,
	price bigint not null,
	transaction_date timestamptz not null,
	postcode varchar not null,
	property_type varchar not null,
	new_tag varchar not null,
	lease varchar not null,
	primary_address_object_name varchar not null,
	secondary_address_object_name varchar not null,
	street varchar not null,
	locality varchar not null,
	town_city varchar not null,
	district varchar not null,
	county varchar not null,
	ppd_cat varchar not null,
	record_status varchar not null,
	is_deleted boolean not null,
	created_datetime timestamptz not null,
	updated_datetime timestamptz,
	deleted_datetime timestamptz
);

alter table land_registry.price_paid_data
add constraint price_paid_data_transaction_unique_id_unique
unique (transaction_unique_id);

create table land_registry.price_paid_data_log (
	price_paid_data_log_id bigserial not null primary key,
	log_timestamp timestamptz not null,
	query_row_count_total bigint not null,
	query_row_count_inserted bigint not null,
	query_row_count_updated bigint not null,
	query_row_count_deleted bigint not null,
	row_count_before bigint not null,
	row_count_after bigint not null
);
