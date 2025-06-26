import csv
import logging
import tempfile
import uuid
from pathlib import Path
from time import sleep

from google.cloud import storage
from sqlalchemy import delete, select
from sqlalchemy.orm import Session
from structlog import wrap_logger

from config import Config
from srm_autoprocessor.db import engine
from srm_autoprocessor.models.job import Job
from srm_autoprocessor.models.job_row import JobRow

logger = wrap_logger(logging.getLogger(__name__))

CHUNK_SIZE = 500


def run_app():
    logger.info("App is running")
    logger.info("Check to see if any jobs available")
    while True:
        with Session(engine) as session:
            stmt = select(Job).where(Job.job_status.in_(["FILE_UPLOADED", "STAGING_IN_PROGRESS", "VALIDATED_OK"]))
            jobs = session.execute(stmt).scalars().all()
            print(f"Jobs available: {len(jobs)}")
            for job in jobs:
                print(f"Job id: {job.id}, status: {job.job_status}, type: {job.job_type}, file name: {job.file_name}")
                if job.job_status in ["FILE_UPLOADED", "STAGING_IN_PROGRESS"]:
                    job_file = get_file_path(job)
                    if job_file is None:
                        logger.error(f"File {job.file_name} does not exist for job {job.id}")
                        continue
                if job.job_status == "FILE_UPLOADED":
                    job_status = process_file_with_header(job, job_file)

                    job.job_status = job_status
                    session.commit()
                    handle_file(job_file)
                elif job.job_status == "STAGING_IN_PROGRESS":
                    logger.info(f"Job {job.id} is in staging, processing file")
                    job_status = staging_job_rows(job, job_file, session)
                    job.job_status = job_status
                    session.commit()
                    handle_file(job_file)
                elif job.job_status == "VALIDATED_OK":
                    job.job_status = "PROCESSING_IN_PROGRESS"
                    session.commit()

        sleep(5)


def handle_file(job_file):
    if Config.RUN_MODE == "CLOUD":
        job_file.unlink(missing_ok=True)  # Remove the temporary file if it exists


def get_file_path(job):
    if Config.RUN_MODE == "CLOUD":
        client = storage.Client()
        bucket = client.bucket(Config.SAMPLE_LOCATION)
        blob = bucket.blob(job.file_name)
        if not blob.exists():
            logger.error(f"File {job.file_name} does not exist in bucket {Config.SAMPLE_LOCATION}")
            return None
        with tempfile.NamedTemporaryFile(delete=False) as temp:
            blob.download_to_filename(temp.name)
        return Path(temp.name)
    else:
        file_path = Path(Config.SAMPLE_LOCATION) / job.file_name
        return file_path


def process_file_with_header(job, job_file):
    if job.collection_exercise.survey.sample_with_header_row:
        logger.info(f"Job {job.id} has a header row, processing file with header")
        with open(job_file, newline="", encoding="utf-8-sig") as file:
            csvfile = csv.reader(file, delimiter=",")
            header = next(csvfile)
            expected_columns = [
                validation_rule["columnName"]
                for validation_rule in job.collection_exercise.survey.sample_validation_rules
            ]
            print(expected_columns)
            if len(header) != len(expected_columns):
                logger.error(f"Header row does not match expected columns for job {job.id}")
                job_status = "VALIDATED_TOTAL_FAILURE"
                job.fatal_error_description = "Header row does not have expected number of columns"
                return job_status
            else:
                for header_row, expected_column in zip(header, expected_columns):
                    if header_row != expected_column:
                        logger.error(
                            f"Header row {header_row} does not match expected column {expected_column} for job {job.id}"
                        )
                        job_status = "VALIDATED_TOTAL_FAILURE"
                        job.fatal_error_description = (
                            f"Header row {header_row} does not match expected column {expected_column}"
                        )
                        return job_status

        return "STAGING_IN_PROGRESS"
    return "STAGING_IN_PROGRESS"


def staging_job_rows(job, job_file, session):
    with open(job_file, newline="", encoding="utf-8-sig") as file:
        csvfile = csv.reader(file, delimiter=",")
        header = next(csvfile)
        header_row_correction = 1

        for _ in range(0, job.staging_row_number):
            next(csvfile)
            # TODO handle without header row?
        while job.staging_row_number < job.file_row_count - header_row_correction:
            job_status = staging_chunks(csvfile, header, job, session)
            if job_status == "VALIDATED_TOTAL_FAILURE":
                break
        if job_status == "VALIDATED_TOTAL_FAILURE":
            stmt = delete(JobRow).where(JobRow.job_id == job.id)
            session.execute(stmt)
            logger.error(f"Job {job.id} has failed validation, deleting all job rows")
        return job_status


def staging_chunks(csvfile, header, job, session):
    job_rows = []
    i = 0
    while i < CHUNK_SIZE:
        try:
            line = next(csvfile)
        except StopIteration:
            break

        # TODO Error handling for empty chunks
        if len(line) != len(header):
            logger.error("CSV corrupt: row data does not match columns")
            job_status = "VALIDATED_TOTAL_FAILURE"
            job.fatal_error_description = "CSV corrupt: row data does not match columns"
            return job_status
        job.staging_row_number += 1
        job_row = JobRow(
            job_row_status="STAGED",
            original_row_data=",".join(line).encode("utf-8"),
            original_row_line_number=job.staging_row_number,
            row_data={header[i]: line[i] for i in range(len(header))},
            job_id=job.id,
            id=uuid.uuid4(),
        )
        job_rows.append(job_row)
        i += 1
    session.add_all(job_rows)
    session.commit()
    return "VALIDATION_IN_PROGRESS"
