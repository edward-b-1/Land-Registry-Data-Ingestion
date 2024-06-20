
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
	created_datetime timestamptz not null,
	updated_datetime timestamptz
);

alter table land_registry.price_paid_data
add constraint price_paid_data_transaction_unique_id_unique
unique (transaction_unique_id);

alter table land_registry.price_paid_data
add constraint price_paid_data_unique
unique (
	transaction_unique_id,
	price,
	transaction_date,
	postcode,
	property_type,
	new_tag,
	lease,
	primary_address_object_name,
	secondary_address_object_name,
	street,
	locality,
	town_city,
	district,
	county,
	ppd_cat,
	record_status
);
