

from datetime import date
from datetime import datetime
from datetime import timedelta

from typeguard import typechecked


@typechecked
def calculate_date_nth_working_day_of_month(
    current_date: date,
    nth: int,
) -> date:
    month_start_date = (
        date(
            year=current_date.year,
            month=current_date.month,
            day=1,
        )
    )
    the_date = month_start_date

    weekday_count = 0

    while True:
        if weekday_count == nth:
            return the_date

        if the_date.weekday() < 5:
            weekday_count += 1

        the_date += timedelta(days=1)

        if the_date.month != month_start_date.month:
            raise RuntimeError(f'{nth} weekday of month starting at date {month_start_date} does not exist')


def convert_to_data_publish_datestamp(input: datetime) -> date:
    day = input.day
    if day < 20:
        month = input.month
        # TODO: used elsewhere but not sure if months start from 0 or 1
        # apparently months start from 1
        if month <= 1:
            year = input.year - 1
            return (
                date(
                    year=year,
                    month=12,
                    day=20,
                )
            )
        else:
            year = input.year
            month = input.month - 1
            return (
                date(
                    year=year,
                    month=month,
                    day=20,
                )
            )
    else:
        return(
            date(
                year=input.year,
                month=input.month,
                day=20,
            )
        )


def last_day_of_month(any_day):
    # The day 28 exists in every month. 4 days later, it's always next month
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
    # subtracting the number of the current day brings us back one month
    return next_month - datetime.timedelta(days=next_month.day)


def convert_to_data_threshold_datestamp(input: datetime) -> date:
    data_publish_datestamp = convert_to_data_publish_datestamp(input)
    return last_day_of_month(data_publish_datestamp)
    # data_publish_datestamp = convert_to_data_publish_datestamp(input)
    # year = data_publish_datestamp.year
    # month = data_publish_datestamp.month
    # day = last_day_of_month(input)

    # return (
    #     date(
    #         year=
    #         month=
    #         day=day,
    #     )
    # )

