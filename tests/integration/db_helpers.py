import datetime
import uuid
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.orm import Session

from srm_autoprocessor.db import engine
from srm_autoprocessor.models.collection_exercise import CollectionExercise
from srm_autoprocessor.models.job import Job
from srm_autoprocessor.models.survey import Survey


def clear_db():
    """Truncate all tables which are written to in the integration tests.
    This must be called from within an active app context.
    """
    with Session(engine) as session:
        session.execute(text("TRUNCATE casev3.survey CASCADE;"))
        session.commit()


def set_up_survey(survey_id: UUID, name: str, metadata: dict, sample_validation_rules: list, session) -> Survey:
    """Helper function for saving a survey to the database.
    This must be called from within an active app context.
    """
    survey = Survey(
        id=survey_id,
        name=name,
        survey_metadata=metadata,
        sample_separator=",",
        sample_validation_rules=sample_validation_rules,
        sample_with_header_row=True,
        sample_definition_url="foo://bar",
    )
    session.add(survey)
    session.commit()
    return survey


def set_up_collection_exercise(collection_exercise_id: UUID, survey_id: UUID, session) -> CollectionExercise:
    """Helper function for saving a collection exercise to the database.
    This must be called from within an active app context.
    """
    collection_exercise = CollectionExercise(
        id=collection_exercise_id,
        survey_id=survey_id,
        name="example_1",
        collection_instrument_selection_rules=["rules"],
        reference="test",
        start_date="2025-01-01",
        end_date="2045-01-01",
        collection_exercise_metadata={"metadata": "example"},
    )
    session.add(collection_exercise)
    session.commit()
    return collection_exercise


def set_up_job(collection_exercise_id: UUID, file_name: str, file_row_count: int, session) -> Job:
    job = Job(
        id=uuid.uuid4(),
        collection_exercise_id=collection_exercise_id,
        created_at=datetime.datetime.now(datetime.timezone.utc),
        last_updated_at=datetime.datetime.now(datetime.timezone.utc),
        file_name=file_name,
        file_id=uuid.uuid4(),
        file_row_count=file_row_count,
        error_row_count=0,
        staging_row_number=0,
        validating_row_number=0,
        processing_row_number=0,
        job_status="FILE_UPLOADED",
        job_type="SAMPLE",
        created_by="test_user",
    )

    session.add(job)
    session.commit()
    return job
