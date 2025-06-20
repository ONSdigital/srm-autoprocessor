import logging
from time import sleep

from sqlalchemy import select
from sqlalchemy.orm import Session
from structlog import wrap_logger

from srm_autoprocessor.db import engine
from srm_autoprocessor.models.job import Job

logger = wrap_logger(logging.getLogger(__name__))


def run_app():
    while True:
        logger.info("App is running")
        logger.info("Check to see if any jobs available")
        #TODO Use SQLAlchemy to get list of jobs
        with Session(engine) as session:
            stmt = select(Job).where(Job.job_status == "FILE_UPLOADED")
            jobs = session.execute(stmt).scalars().all()
            print(f"Jobs available: {len(jobs)}")
            for job in jobs:
                print(f"Job id: {job.id}, status: {job.job_status}, type: {job.job_type}, file name: {job.file_name}")

        #TODO Switch statement to handle job types

        sleep(1)


if __name__ == "__main__":
    run_app()
