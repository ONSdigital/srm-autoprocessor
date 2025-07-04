import json
import logging

import pytest
import structlog
from structlog import wrap_logger

from srm_autoprocessor.logger_config import logger_initial_config


@pytest.fixture
def configured_logger() -> structlog.stdlib.BoundLogger:
    """Set and configure logger with our application default settings."""
    logger_initial_config()
    logger = wrap_logger(logging.getLogger("Test Logger"))
    return logger


def test_logger_config(caplog, configured_logger):
    # Given
    caplog.set_level(logging.INFO)
    test_log_message = "Test log line"
    test_log_value = "test_value"

    # When
    configured_logger.info(test_log_message, test_key=test_log_value)

    # Then
    assert len(caplog.messages) == 1, "Expect only one log message"
    log_line = caplog.messages[0]

    # We expect the log lines to be in JSON format
    log_contents = json.loads(log_line)
    assert log_contents["event"] == test_log_message, "Expect the message to be logged in the 'event' key"
    assert log_contents["test_key"] == test_log_value, "Expect keys and values to be logged correctly"
    assert log_contents["level"] == "info", "Expect the log level to be info"
    assert log_contents["severity"] == "info", "Expect the log severity to be 'INFO'"
    assert log_contents["service"] == "autoprocessor", "Expect the service name to be logged in the 'service' key"


def test_logger_warn_severity(caplog, configured_logger):
    # Given
    caplog.set_level(logging.WARNING)

    # When
    configured_logger.warn("foo")

    # Then
    assert len(caplog.messages) == 1, "Expect only one log message"
    log_line = caplog.messages[0]
    log_contents = json.loads(log_line)
    assert log_contents["level"] == "warning", "Expect the full 'warning' level to be logged when using the warn alias"
    assert (
        log_contents["severity"] == "warning"
    ), "Expect the full 'WARNING' severity to be logged when using the warn alias"


def test_logging_an_exception(caplog, configured_logger):
    # Given
    caplog.set_level(logging.ERROR)
    test_log_message = "Text exception message"

    # When
    try:
        raise ValueError("Forced exception")
    except ValueError as test_exception:
        configured_logger.exception(test_log_message, exception=test_exception)

    # Then
    assert len(caplog.messages) == 1, "Expect only one log message"
    log_line = caplog.messages[0]
    log_contents = json.loads(log_line)

    assert type(log_contents["exception"]) is list, "Expect 'exception' to be a list"
    assert log_contents["exception"][0].startswith("Traceback"), "Expect the stack trace to be logged"
    assert log_contents["severity"] == "error", "Expect the severity to be 'ERROR'"
    assert log_contents["event"] == test_log_message, "Expect the message to be logged in the 'event' key"
