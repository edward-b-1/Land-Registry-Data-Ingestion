#!/usr/bin/env python3


import pandas
from sqlalchemy import Connection
from sqlalchemy import create_engine
from sqlalchemy import text


def main():

    #check_number_of_rows_in_dataframe_returned_by_database()

    url = 'postgresql://user:password@host/postgres'
    engine_postgres = create_engine(url)

    with engine_postgres.connect() as connection:

        (price_paid_data_set, price_paid_data_set_duplicates) = \
            get_transaction_unique_id_set(connection, 'price_paid_data')
        print(f'price_paid_data_set len: {len(price_paid_data_set)}')
        print(f'price_paid_data_set_duplicates len: {len(price_paid_data_set_duplicates)}')

        (price_paid_data_raw_set, price_paid_data_raw_set_duplicates) = \
            get_transaction_unique_id_set(connection, 'price_paid_data_raw')
        print(f'price_paid_data_raw_set len: {len(price_paid_data_raw_set)}')
        print(f'price_paid_data_raw_set_duplicates len: {len(price_paid_data_raw_set_duplicates)}')

        price_paid_data_raw_set_missing_ids = set()
        for id in price_paid_data_set:
            if not id in price_paid_data_raw_set:
                #print(f'price_paid_data_raw_set is missing id: {id}')
                price_paid_data_raw_set_missing_ids.add(id)

        print(f'there are {len(price_paid_data_raw_set_missing_ids)} missing ids in price_paid_data_raw')

        with open('price_paid_data_raw_set_missing_ids.txt', 'w') as f:
            for id in price_paid_data_raw_set_missing_ids:
                f.write(f"{id}\n")


        price_paid_data_set_missing_ids = set()
        for id in price_paid_data_raw_set:
            if not id in price_paid_data_set:
                #print(f'price_paid_data_set is missing id: {id}')
                price_paid_data_set_missing_ids.add(id)

        print(f'there are {len(price_paid_data_set_missing_ids)} missing ids in price_paid_data')

        with open('price_paid_data_set_missing_ids.txt', 'w') as f:
            for id in price_paid_data_set_missing_ids:
                f.write(f"{id}\n")

        # get the data for rows missing from price_paid_data
        missing_data = []

        for id in price_paid_data_raw_set_missing_ids:
            query_string = text(
                f'''
                    select * from land_registry.price_paid_data
                    where transaction_unique_id = :transaction_unique_id
                '''
            )
            query_params = { 'transaction_unique_id': id }
            query_result = connection.execute(query_string, query_params)
            # print(type(query_result))
            # for d in dir(query_result):
            #     if not d.startswith('__'):
            #         print(d)

            scalar_query_result = query_result.one()

            missing_data.append(
                {
                    'transaction_unique_id': scalar_query_result.transaction_unique_id,
                    'price': scalar_query_result.price,
                    'transaction_date': scalar_query_result.transaction_date,
                    'postcode': scalar_query_result.postcode,
                    'property_type': scalar_query_result.property_type,
                    'new_tag': scalar_query_result.new_tag,
                    'lease': scalar_query_result.lease,
                    'primary_address_object_name': scalar_query_result.primary_address_object_name,
                    'secondary_address_object_name': scalar_query_result.secondary_address_object_name,
                    'street': scalar_query_result.street,
                    'locality': scalar_query_result.locality,
                    'town_city': scalar_query_result.town_city,
                    'district': scalar_query_result.district,
                    'county': scalar_query_result.county,
                    'ppd_cat': scalar_query_result.ppd_cat,
                    'record_status': scalar_query_result.record_status,
                }
            )

        df_missing_data = pandas.DataFrame(missing_data)

        for index, row in df_missing_data.iterrows():
            print(
                f'{row["transaction_unique_id"]} '
                f'{row["transaction_date"]} '
                f'{row["property_type"]} '
                f'{row["new_tag"]} '
                f'{row["lease"]} '
                f'{row["ppd_cat"]} '
                f'{row["record_status"]}'
            )

        df_missing_data.to_parquet('df_missing_data.parquet')
        df_missing_data.to_csv('df_missing_data.csv')
        #df_missing_data.to_excel('df_missing_data.xlsx')



def get_transaction_unique_id_set(connection: Connection, table_name: str):

    transaction_unique_id_set = set()
    transaction_unique_id_set_duplicates = set()

    query_string = text(
        f'''
            select transaction_unique_id
            from land_registry.{table_name}
        '''
    )

    query_params = {}

    query_result = connection.execute(query_string, query_params)

    for row in query_result:
        transaction_unique_id = row.transaction_unique_id

        if not transaction_unique_id in transaction_unique_id_set:
            transaction_unique_id_set.add(transaction_unique_id)
        else:
            transaction_unique_id_set_duplicates.add(transaction_unique_id)
            #raise RuntimeError(f'duplicate transaction unique id: {transaction_unique_id}')

    return (transaction_unique_id_set, transaction_unique_id_set_duplicates)


# returns 28951515
def check_number_of_rows_in_dataframe_returned_by_database():

    url = 'postgresql://user:password@host/postgres'
    engine_postgres = create_engine(url)

    df = pandas.read_sql_table(
        table_name='price_paid_data',   # _raw
        schema='land_registry',
        con=engine_postgres,
    )

    print(len(df))



if __name__ == '__main__':
    main()

