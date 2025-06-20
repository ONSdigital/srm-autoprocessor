import logging
from time import sleep

from structlog import wrap_logger

logger = wrap_logger(logging.getLogger(__name__))


def run_app():
    while True:
        logger.info("App is running")
        logger.info("Check to see if any jobs available")
        #TODO Use SQLAlchemy to get list of jobs

        #TODO Switch statement to handle job types

        sleep(5)


if __name__ == "__main__":
    run_app()
