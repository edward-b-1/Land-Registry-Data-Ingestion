
from datetime import datetime
from datetime import timezone
from datetime import timedelta

import croniter
import time


def cron_get_next_schedule(now: datetime) -> datetime:

    # everyday at 00:00
    iter = croniter.croniter('0 0 * * *', now)
    next_schedule = iter.get_next(datetime)

    return next_schedule


def cron_get_sleep_time(now: datetime, next_schedule: datetime) -> timedelta:

    # calculate time to sleep
    sleep_timedelta = next_schedule - now
    return sleep_timedelta


def cron_get_sleep_time_timeout(
    now: datetime,
    next_schedule: datetime,
    timeout: timedelta,
) -> timedelta:
    if now >= next_schedule:
        return timedelta(seconds=0)

    # calculate time to sleep
    sleep_timedelta = next_schedule - now
    sleep_timedelta = min(sleep_timedelta, timeout)
    return sleep_timedelta


def cron_do_sleep(sleep_timedelta: timedelta):
    sleep_time = sleep_timedelta.total_seconds()
    time.sleep(sleep_time)
