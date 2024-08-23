
from datetime import datetime
from datetime import date
from datetime import timedelta

from dataclasses import dataclass


@dataclass
class CronTriggerNotificationDTO():
    '''
    DTO produced by CRON trigger process
    '''

    notification_source: str
    notification_type: str
    notification_timestamp: datetime
    
    pp_complete_file_log_id: int
    pp_monthly_update_file_log_id: int


@dataclass
class PPCompleteDownloadCompleteNotificationDTO():
    '''
    Notification that Price Paid Complete (pp-complete) data download
    process has completed
    '''

    notification_source: str
    notification_type: str
    notification_timestamp: datetime
    
    pp_complete_file_log_id: int
    

@dataclass
class PPMonthlyUpdateDownloadCompleteNotificationDTO():
    '''
    Notification that Price Paid Monthly Update data download process
    has completed
    '''

    notification_source: str
    notification_type: str
    notification_timestamp: datetime
    
    pp_monthly_update_file_log_id: int


@dataclass
class PPCompleteDataDecisionNotificationDTO():
    '''
    Notification that process to query database records and make
    upload/discard data decision has completed
    '''

    notification_source: str
    notification_type: str
    notification_timestamp: datetime
    
    pp_complete_file_log_id: int


@dataclass
class PPMonthlyUpdateDataDecisionNotificationDTO():
    '''
    Notification that process to query database records and make
    upload/discard data decision has completed
    '''

    notification_source: str
    notification_type: str
    notification_timestamp: datetime
    
    pp_monthly_update_file_log_id: int


@dataclass
class PPCompleteGCNotificationDTO():
    '''
    Notification that garbage collection process has been completed
    '''

    notification_source: str
    notification_type: str
    notification_timestamp: datetime
    
    pp_complete_file_log_id: int


@dataclass
class PPMonthlyUpdateGCNotificationDTO():
    '''
    Notification that garbage collection process has been completed
    '''

    notification_source: str
    notification_type: str
    notification_timestamp: datetime
    
    pp_monthly_update_file_log_id: int


@dataclass
class PPCompleteArchiveNotificationDTO():
    '''
    Notification that archive process has been completed
    '''

    notification_source: str
    notification_type: str
    notification_timestamp: datetime
    
    pp_complete_file_log_id: int


@dataclass
class PPMonthlyUpdateArchiveNotificationDTO():
    '''
    Notification that archive process has been completed
    '''

    notification_source: str
    notification_type: str
    notification_timestamp: datetime
    
    pp_monthly_update_file_log_id: int


# @dataclass
# class MonthlyUpdateDatabaseUpdateCompleteNotificationDTO():
#     '''
#     Notification that process to upload incremental monthly update
#     data to database has completed
#     '''

#     notification_source: str
#     notification_type: str
#     notification_timestamp: datetime
    
#     filename: str
#     sha256sum: str
#     data_decision: str
#     file_row_count: int
#     file_row_count_insert: int
#     file_row_count_change: int
#     file_row_count_delete: int
#     database_row_count_before: int
#     database_row_count_after: int

#     timestamp_cron_trigger: datetime
#     timestamp_download: datetime
#     timestamp_shasum: datetime
#     timestamp_data_decision: datetime
#     timestamp_database_upload: datetime
