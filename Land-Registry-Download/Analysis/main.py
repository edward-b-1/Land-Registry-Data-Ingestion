#!/usr/bin/env python3

from sqlalchemy import create_engine
from sqlalchemy import text

import os
import pandas
import matplotlib.pyplot as plt

from datetime import datetime


def main():

    postgres_address = os.environ['POSTGRES_ADDRESS']
    postgres_user = os.environ['POSTGRES_USER']
    postgres_password = os.environ['POSTGRES_PASSWORD']
    postgres_database = os.environ['POSTGRES_DATABASE']
    postgres_connection_string = f'postgresql://{postgres_user}:{postgres_password}@{postgres_address}/{postgres_database}'

    #url = 'postgresql://user:password@host/postgres'
    engine_postgres = create_engine(postgres_connection_string)

    query = text(
        '''
            select
                created_datetime,
                updated_datetime,
                deleted_datetime,
                transaction_date
            from
                land_registry.price_paid_data
            where
                is_deleted = 'F'
        '''
    )
    df = pandas.read_sql(query, engine_postgres)
    df['created_datetime'] = pandas.to_datetime(df['created_datetime'], utc=True)
    df['updated_datetime'] = pandas.to_datetime(df['updated_datetime'], utc=True)
    df['deleted_datetime'] = pandas.to_datetime(df['deleted_datetime'], utc=True)
    df['transaction_date'] = pandas.to_datetime(df['transaction_date'], utc=True)

    print('check for deleted datetime:')
    print(df['deleted_datetime'].unique())

    print(f'created_datetime:')
    print(df['created_datetime'].unique())
    print(len(df['created_datetime'].unique()))

    df['created_datetime_transaction_date_delay'] = (
        df['created_datetime'] - df['transaction_date']
    )

    print('columns:')
    print(df.columns)
    print(df.dtypes)

    df['delay_days'] = df['created_datetime_transaction_date_delay'].dt.days

    print(df.head())

    fig, ax = plt.subplots(1)

    ax.hist(
        df['delay_days'],
        bins=100,
    )
    fig.savefig('delay_days.png')
    fig.savefig('delay_days.pdf')

    df = df[df['delay_days'] < 1000]

    fig, ax = plt.subplots(1)

    ax.hist(
        df['delay_days'],
        bins=100,
    )
    fig.savefig('delay_days_lt_1000.png')
    fig.savefig('delay_days_lt_1000.pdf')



if __name__ == '__main__':
    main()

