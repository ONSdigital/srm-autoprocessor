import csv
import logging
from pathlib import Path
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
            stmt = select(Job).where(Job.job_status.in_(["FILE_UPLOADED","VALIDATED_OK"]))
            jobs = session.execute(stmt).scalars().all()
            print(f"Jobs available: {len(jobs)}")
            for job in jobs:
                print(f"Job id: {job.id}, status: {job.job_status}, type: {job.job_type}, file name: {job.file_name}")
                if job.job_status == "FILE_UPLOADED":
                    job_file = Path(f"./sample_files/{job.file_name}")
                    if not job_file.exists():
                        logger.error(f"File {job.file_name} does not exist for job {job.id}")
                        continue
                    if job.collection_exercise.survey.sample_with_header_row:
                        logger.info(f"Job {job.id} has a header row, processing file with header")
                        # Process file with header
                        with open(job_file, "r", newline="") as file:
                            csvfile = csv.reader(file, delimiter=",")
                            header = next(csvfile)
                            expected_columns = [validation_rule["columnName"] for validation_rule in job.collection_exercise.survey.sample_validation_rules]
                            print(expected_columns)
                            if len(header) != len(expected_columns):
                                logger.error(f"Header row does not match expected columns for job {job.id}")
                                # TODO handle error
                                continue
                            else:
                                for header_row, expected_column in zip(header, expected_columns):
                                    if header_row != expected_column:
                                        logger.error(f"Header row {header_row} does not match expected column {expected_column} for job {job.id}")
                                        # TODO handle error
                                        continue

                    job.job_status = "STAGING_IN_PROGRESS"

                    session.commit()
                elif job.job_status == "STAGING_IN_PROGRESS":
                    logger.info(f"Job {job.id} is in staging, processing file")
                    job_file = Path(f"./sample_files/{job.file_name}")
                    if not job_file.exists():
                        logger.error(f"File {job.file_name} does not exist for job {job.id}")
                        continue
                    with open(job_file, "r", newline="") as file:
                        csvfile = csv.reader(file, delimiter=",")
                        header = next(csvfile)
                        expected_columns = [validation_rule["columnName"] for validation_rule in
                                            job.collection_exercise.survey.sample_validation_rules]
                        print(expected_columns)
                        header_row_correction = 1
                        # TODO handle without header row?


        #TODO Switch statement to handle job types

        sleep(1)
