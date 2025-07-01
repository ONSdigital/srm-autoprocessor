import uuid
from time import sleep

from sqlalchemy import select
from sqlalchemy.orm import Session

from srm_autoprocessor.db import engine
from srm_autoprocessor.models import Job, JobRow
from tests.integration.db_helpers import set_up_collection_exercise, set_up_job, set_up_survey


def test_processing_jobs(run_test_app):
    sample_validation_rules = [
        {
            "columnName": "emailAddress",
            "rules": [{"className": "uk.gov.ons.ssdc.common.validation.EmailRule", "mandatory": True}],
            "sensitive": True,
        }
    ]
    with Session(engine) as session:

        survey = set_up_survey(
            uuid.uuid4(),
            name="test survey",
            metadata={"key": "value"},
            sample_validation_rules=sample_validation_rules,
            session=session,
        )
        collection_exercise = set_up_collection_exercise(uuid.uuid4(), survey_id=survey.id, session=session)
        job = set_up_job(collection_exercise.id, file_name="email_driven.csv", file_row_count=6, session=session)
        job_id = job.id
    i = 0
    number_of_iterations = 30
    while i < number_of_iterations:
        with Session(engine) as session:
            stmt = select(Job).where(Job.id == job.id)
            jobs = session.execute(stmt).scalars().all()
            if jobs[0].job_status == "VALIDATION_IN_PROGRESS":
                break
            i += 1
            print(i)
            sleep(1)
    expected_number_of_staged_rows = 5
    assert jobs[0].job_status == "VALIDATION_IN_PROGRESS", "Job did not transition to STAGING_IN_PROGRESS"
    assert jobs[0].staging_row_number == expected_number_of_staged_rows

    with Session(engine) as session:
        stmt = select(JobRow).where(JobRow.job_id == job_id)
        job_rows = session.execute(stmt).scalars().all()
    assert len(job_rows) == expected_number_of_staged_rows
