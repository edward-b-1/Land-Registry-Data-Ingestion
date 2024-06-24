#!/usr/bin/env python3

from sqlalchemy import create_engine
from sqlalchemy import text

import os
import pandas
import matplotlib.pyplot as plt

from datetime import datetime
from datetime import timezone


# it appears I was doing something very stupid here

# I first created some bins from the created_datetime column
# I then binned the created_datetime column into bins using pandas.cut, using the
# bin values from the created datetime column. This clearly doesn't make any sense


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

    created_datetime_bins = (
        sorted(
            list(
                df['created_datetime'].unique()
            )
            + [datetime(year=1900, month=1, day=1, tzinfo=timezone.utc)]
        )
    )
    print(type(created_datetime_bins))
    print(created_datetime_bins)
    print(len(created_datetime_bins))

    created_datetime_counts, created_datetime_bins_2 = pandas.cut(
        df['created_datetime'],
        bins=created_datetime_bins,
        retbins=True,
        #ordered=False,
    )
    print(f'type(created_datetime_counts)')
    print(type(created_datetime_counts))
    print(f'created_datetime_counts')
    print(created_datetime_counts)
    print(f'len(created_datetime_counts)')
    print(len(created_datetime_counts))

    print(f'type(created_datetime_bins_2)')
    print(type(created_datetime_bins_2))
    print(f'created_datetime_bins_2')
    print(created_datetime_bins_2)
    print(f'len(created_datetime_bins_2)')
    print(len(created_datetime_bins_2))

    df['created_datetime_bin'] = created_datetime_counts
    histogram_counts = df['created_datetime_bin'].value_counts().sort_index()
    histogram_bins = created_datetime_bins_2

    histogram_df = histogram_counts.reset_index()
    histogram_df.columns = ['threshold', 'count']

    histogram_df['threshold_right'] = histogram_df['threshold'].map(lambda interval: interval.right)
    histogram_df

    # fig, ax = plt.subplots(1)
    # ax.hist(
    #     histogram_df['count'],
    #     bins=histogram_df['threshold_right'],
    # )
    # fig.savefig('delay_bin.png')
    # fig.savefig('delay_bin.pdf')

    # above few lines are pointless

    df['created_datetime_bin_right'] = df['created_datetime_bin'].map(lambda interval: interval.right)
    # long way round conversion to de-categorize data
    #df['created_datetime_bin_right_decat_str'] = df['created_datetime_bin_right'].astype(str)
    #df['created_datetime_bin_right_decat_str_datetime'] = pandas.to_datetime(df['created_datetime_bin_right_decat_str'])

    # alternative
    df['created_datetime_bin_right_decat'] = df['created_datetime_bin_right'].astype('datetime64[ns, UTC]')

    #df['delay'] = df['created_datetime_bin_right_decat_str_datetime'] - df['transaction_date']
    df['delay'] = df['created_datetime_bin_right_decat'] - df['transaction_date']

    print(df)

    df['delay_days'] = df['delay'].dt.days
    df['delay_weeks'] = df['delay_days'] // 7
    df_filter = df[df['delay_weeks'] < 52 * 3]

    print(df_filter)

    fig, ax = plt.subplots(1)
    ax.hist(
        df_filter['delay_weeks'],
        bins=52,
    )
    fig.savefig('delay_bin.png')
    fig.savefig('delay_bin.pdf')


if __name__ == '__main__':
    main()

