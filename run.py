import logging

from structlog import wrap_logger

from srm_autoprocessor.logger_config import logger_initial_config
from srm_autoprocessor.run import run_app


def run():
    logger_initial_config()
    logger = wrap_logger(logging.getLogger(__name__))
    logger.info("Starting app")
    run_app()


if __name__ == '__main__':
    run()
