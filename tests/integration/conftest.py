import logging
from multiprocessing import Process
from pathlib import Path
from time import sleep

import pytest
from structlog import wrap_logger

from config import config
from run import run
from tests.integration.db_helpers import clear_db

logger = wrap_logger(logging.getLogger(__name__))
MAX_APP_READINESS_ATTEMPTS = 100


@pytest.fixture
def run_test_app():
    logger.info("TEST SETUP: Initialising and cleaning directories and database")

    test_app = Process(target=run)

    logger.info("TEST SETUP: Starting test app in sub process")
    test_app.start()

    try:
        logger.info("TEST SETUP: Waiting for test app to show ready")
        wait_for_readiness()
        logger.info("TEST SETUP: Test app is ready")
    except Exception:
        logger.error("TEST ERROR: Test app failed to start up in time")
        terminate_test_app(test_app)
        raise

    yield

    terminate_test_app(test_app)

    logger.info("TEST CLEAR DOWN: Clearing down directories and database")
    clear_db()
    readiness_file = Path(config.READINESS_FILE_PATH)
    if readiness_file.exists():
        readiness_file.unlink()


def terminate_test_app(test_app: Process):
    logger.info("TEST CLEAR DOWN: Terminating test app")
    test_app.terminate()

    # Give the app up to 10 seconds to terminate gracefully
    test_app.join(timeout=10)

    # Forcefully kill the process in case it didn't shutdown gracefully in time
    test_app.kill()
    test_app.close()


def wait_for_readiness():
    for _ in range(MAX_APP_READINESS_ATTEMPTS):
        if config.READINESS_FILE_PATH.exists():
            break
        sleep(0.1)
    else:
        raise RuntimeError("Ran out of attempts waiting for test app to start")
