

from datetime import datetime
from datetime import date

import sys
import logging
from logging import Logger
from logging import Handler

from typeguard import typechecked

from dataclasses import dataclass


@dataclass
class LoggerWrapper():
    logger_: Logger


logger_process_name = None
logger = LoggerWrapper(
    logger_=None,
)


@typechecked
def set_logger_process_name(process_name: str) -> None:
    global logger_process_name
    global logger

    logger_process_name = process_name
    logger.logger_ = None

    _initialize_logger()


@typechecked
def create_stdout_log_handler() -> Handler:
    stdout_log_formatter = logging.Formatter(
        '%(name)s: %(asctime)s | %(levelname)s | %(filename)s:%(lineno)s | %(process)d | %(message)s'
    )

    stdout_log_handler = logging.StreamHandler(stream=sys.stdout)
    stdout_log_handler.setLevel(logging.INFO)
    stdout_log_handler.setFormatter(stdout_log_formatter)

    return stdout_log_handler


@typechecked
def create_file_log_handler(
    logger_process_name: str,
    logger_file_datetime: datetime|date,
) -> Handler:
    file_log_formatter = logging.Formatter(
        '%(name)s: %(asctime)s | %(levelname)s | %(filename)s:%(lineno)s | %(process)d | %(message)s'
    )

    file_log_handler = logging.FileHandler(
        filename=f'{logger_process_name}_{logger_file_datetime}.log'
    )
    file_log_handler.setLevel(logging.DEBUG)
    file_log_handler.setFormatter(file_log_formatter)

    return file_log_handler


@typechecked
def get_logger() -> Logger:
    global logger

    if logger.logger_ is not None:
        return logger.logger_

    if logger.logger_ is None:
        _initialize_logger()

    return logger.logger_


@typechecked
def _initialize_logger():
    global logger
    global logger_process_name

    if logger_process_name is None:
        # a sensible default which permits initialization of the module
        logger_name = __name__
    else:
        logger_name = logger_process_name

    logger.logger_ = logging.getLogger(logger_name)
    logger.logger_.setLevel(logging.DEBUG)

