import datetime
import uuid
from uuid import UUID

from sqlalchemy import text

from srm_autoprocessor.db import engine as db
from srm_autoprocessor.models.job import Job


def clear_db():
    """Truncate all tables which are written to in the integration tests.
    This must be called from within an active app context.
    """
    db.session.execute(text("TRUNCATE casev3.survey CASCADE;"))
    db.session.execute(text("TRUNCATE casev3.email_template CASCADE;"))
    db.session.execute(text("TRUNCATE casev3.action_rule_survey_email_template;"))
    db.session.commit()


def set_up_job(collection_exercise_id: UUID, file_name: str, file_row_count: int) -> Job:
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

    db.session.add(job)
    db.session.commit()
    return job
