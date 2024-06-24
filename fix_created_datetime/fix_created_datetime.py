#!/usr/bin/env python3

import sqlalchemy
import sqlite3

from sqlalchemy import create_engine
from sqlalchemy import text
from datetime import datetime
from datetime import timezone


def main():

    print(f'connecting to sqlite3 database')

    database_filename = '/home/ecb/property-price-analysis-new/data/land_registry_data_daily_feed.sqlite3'

    engine_sqlite = sqlalchemy.create_engine(
        f'sqlite:///{database_filename}',
        execution_options={
            'sqlite_raw_colnames': True,
        },
    )

    with engine_sqlite.connect() as connection:
        query_string = text(
            f'''
                select FirstSeenDatetime, count(FirstSeenDatetime)
                from land_registry_data
                group by FirstSeenDatetime
            '''
        )
        query_params = {}

        result = connection.execute(query_string, query_params)

        for item in result:
            print(f'{item[0]} / {item[1]}')


    print(f'connecting to postgres database')

    url = f'postgresql://postgres:adminpassword@192.168.0.232/postgres'
    engine_postgres = create_engine(url)

    with engine_postgres.connect() as connection:
        query_string = text(
            f'''
                select created_datetime, count(created_datetime)
                from land_registry.price_paid_data
                group by created_datetime
            '''
        )
        query_params = {}

        result = connection.execute(query_string, query_params)

        for item in result:
            print(f'{item[0]} / {item[1]}')


    return


    # fix the data using the sqlite3 data as input
    with engine_sqlite.connect() as connection_sqlite3:
        with engine_postgres.connect() as connection_postgres:
            query_string = text(
                f'''
                    select TUID, FirstSeenDatetime
                    from land_registry_data
                '''
            )
            query_params = {}

            result = connection_sqlite3.execute(query_string, query_params)

            # print_count_interval = result.rowcount // 10
            # print_count = 0

            for item in result:
                # print_count += 1
                # if print_count % print_count_interval == 0:
                #     percent = 100.0 * (print_count / result.rowcount)
                #     print(f'{percent} %')

                tuid = item[0]
                first_seen_datetime = datetime.fromisoformat(item[1]).replace(tzinfo=timezone.utc)
                #.strptime(item[1], '%Y-%m-%d %H:%M:%S.%f')

                query_string = text(
                    f'''
                        update land_registry.price_paid_data
                        set created_datetime = least(created_datetime, :first_seen_datetime)
                        where transaction_unique_id = :tuid
                    '''
                )
                query_params = {
                    'first_seen_datetime': first_seen_datetime,
                    'tuid': tuid,
                }
                result = connection_postgres.execute(query_string, query_params)

            connection_postgres.commit()




if __name__ == '__main__':
    main()
