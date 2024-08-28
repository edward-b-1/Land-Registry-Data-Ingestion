

from datetime import date
from datetime import datetime
from datetime import timedelta

from typeguard import typechecked

from lib_land_registry_data.logging import get_logger

logger = get_logger()


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

    weekday_count = 1

    while True:
        if weekday_count == nth:
            return the_date

        if the_date.weekday() < 5:
            weekday_count += 1

        the_date += timedelta(days=1)

        if the_date.month != month_start_date.month:
            raise RuntimeError(f'{nth} weekday of month starting at date {month_start_date} does not exist')


@typechecked
def convert_data_threshold_datestamp_to_data_publish_datestamp(
    data_threshold_datestamp: date,
) -> date:
    # go forward to next month
    if data_threshold_datestamp.month < 12:
        next_month = (
            date(
                year=data_threshold_datestamp.year,
                month=data_threshold_datestamp.month + 1,
                day=1,
            )
        )
    else:
        next_month = (
            date(
                year=data_threshold_datestamp.year + 1,
                month=1,
                day=1,
            )
        )

    # go to 20th working day of this month
    data_publish_datestamp = calculate_date_nth_working_day_of_month(next_month, nth=20)

    return data_publish_datestamp


def convert_to_data_publish_datestamp(current_date: datetime) -> date:
    # TODO: fix the logging framework
    #logger = get_logger()

    logger.info(f'convert_to_data_publish_datestamp')
    logger.info(f'{current_date=}')
    nth_working_day = calculate_date_nth_working_day_of_month(
        current_date=current_date,
        nth=20,
    )
    logger.info(f'{nth_working_day=}')

    day = current_date.day
    if day < nth_working_day.day:
        month = current_date.month

        if month <= 1:
            year = current_date.year - 1

            publish_date = (
                date(
                    year=year,
                    month=12,
                    day=1,
                )
            )
            publish_date = calculate_date_nth_working_day_of_month(current_date=publish_date, nth=20)
            return publish_date
        else:
            year = current_date.year
            month = current_date.month - 1

            publish_date = (
                date(
                    year=year,
                    month=month,
                    day=1,
                )
            )
            publish_date = calculate_date_nth_working_day_of_month(current_date=publish_date, nth=20)
            return publish_date
    else:
        publish_date = nth_working_day
        return publish_date


def last_day_of_month(any_day):
    # TODO: fix the logging framework
    #logger = get_logger()

    logger.info(f'last_day_of_month')
    # The day 28 exists in every month. 4 days later, it's always next month
    next_month = any_day.replace(day=28) + timedelta(days=4)
    # subtracting the number of the current day brings us back one month
    last_day_of_month_value = next_month - timedelta(days=next_month.day)
    logger.info(f'{any_day} -> {last_day_of_month_value}')
    return last_day_of_month_value


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

