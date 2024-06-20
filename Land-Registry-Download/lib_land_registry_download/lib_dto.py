
from datetime import datetime

from dataclasses import dataclass
from typing import Optional


@dataclass
class CronTriggerNotificationDTO():
    '''
    DTO produced by CRON trigger process
    '''

    notification_source: str
    notification_type: str
    timestamp: datetime


@dataclass
class PPCompleteDownloadCompleteNotificationDTO():
    '''
    Notification that Price Paid Complete (pp-complete) data download
    process has completed
    '''

    notification_source: str
    notification_type: str
    timestamp: datetime
    filename: str # pass this on for GC


@dataclass
class MonthlyUpdateDownloadCompleteNotificationDTO():
    '''
    Notification that Price Paid Monthly Update data download process
    has completed
    '''

    notification_source: str
    notification_type: str
    timestamp: datetime
    filename: str # pass this on for GC


@dataclass
class MonthlyUpdateSHA256CalculationCompleteNotificationDTO():
    '''
    Notification that SHA-256 sum calculation has completed
    '''

    notification_source: str
    notification_type: str
    timestamp: datetime # TODO: think this timestamp value is being set incorrectly? should have a file_timestamp and a message timestamp
    filename: str
    sha256sum: str


@dataclass
class MonthlyUpdateDataDecisionCompleteNotificationDTO():
    '''
    Notification that process to query database records and make
    upload/discard data decision has completed
    '''

    notification_source: str
    notification_type: str
    timestamp: datetime
    filename: str
    sha256sum: str
    data_decision: str


@dataclass
class MonthlyUpdateDatabaseUpdateCompleteNotificationDTO():
    '''
    Notification that process to upload incremental monthly update
    data to database has completed
    '''

    notification_source: str
    notification_type: str
    timestamp: datetime
    filename: str
    sha256sum: str
    data_decision: str
    file_row_count: Optional[int]
    file_row_count_insert: Optional[int]
    file_row_count_change: Optional[int]
    file_row_count_delete: Optional[int]
    database_row_count_before: Optional[int]
    database_row_count_after: Optional[int]


@dataclass
class MonthlyUpdateGarbageCollectorCompleteNotificationDTO():
    '''
    Notification that garbage collection process has been completed
    '''

    # TODO: this is actually being used to notify that a particular
    # file was deleted rather than the process having been run
    # which is a bit strange. it should really just log the datetime
    # for when the process was run

    notification_source: str
    notification_type: str
    timestamp: datetime
    filename: str