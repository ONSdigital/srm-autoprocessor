import uuid
from datetime import datetime, timezone

from srm_autoprocessor.models import CollectionExercise, Job, Survey


def create_survey(survey_name, sample_validation_rules, header_row=True):
    if sample_validation_rules is None:
        sample_validation_rules = [
            {
                "columnName": "emailAddress",
                "rules": [{"className": "uk.gov.ons.ssdc.common.validation.EmailRule", "mandatory": True}],
                "sensitive": True,
            }
        ]
    survey = Survey(
        id=uuid.uuid4(),
        name=survey_name,
        sample_definition_url="",
        sample_separator=",",
        sample_validation_rules=sample_validation_rules,
        sample_with_header_row=header_row,
    )
    return survey


def create_collection_exercise(collection_exercise_name, survey):
    collection_exercise = CollectionExercise(
        id=uuid.uuid4(),
        name=collection_exercise_name,
        reference="foo",
        start_date=datetime(2025, 1, 1, 0, 0, 0),
        end_date=datetime(2025, 1, 2, 23, 59, 59),
        survey_id=survey.id,
        survey=survey,
    )
    return collection_exercise


def create_job(collection_exercise, file_name, file_row_count, job_status="FILE_UPLOADED"):
    job = Job(
        id=uuid.uuid4(),
        collection_exercise_id=collection_exercise.id,
        created_at=datetime.now(timezone.utc),
        last_updated_at=datetime.now(timezone.utc),
        file_name=file_name,
        file_id=uuid.uuid4(),
        file_row_count=file_row_count,
        error_row_count=0,
        staging_row_number=0,
        validating_row_number=0,
        processing_row_number=0,
        job_status=job_status,
        job_type="SAMPLE",
        collection_exercise=collection_exercise,
    )
    return job
