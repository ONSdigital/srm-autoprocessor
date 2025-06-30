import logging
from pathlib import Path

from structlog import wrap_logger

logger = wrap_logger(logging.getLogger(__name__))


class Readiness:
    def __init__(self, readiness_file: Path):
        self.readiness_file = readiness_file

    def __enter__(self):
        self.show_ready()
        return self

    def __exit__(self, *_):
        self.show_unready()

    def show_ready(self):
        logger.debug('Creating readiness file', readiness_file_path=str(self.readiness_file))
        self.readiness_file.touch()

    def show_unready(self):
        logger.debug('Removing readiness file', readiness_file_path=str(self.readiness_file))
        try:
            self.readiness_file.unlink()
        except FileNotFoundError:
            logger.debug('Readiness file not found', readiness_file_path=str(self.readiness_file))
