import uuid
from datetime import datetime, timezone

from srm_autoprocessor.models.job import Job


def test_job_as_dict():
    # Given
    job = Job(
        id=uuid.uuid4(),
        collection_exercise_id=uuid.uuid4(),
        created_at=datetime.now(timezone.utc),
        last_updated_at=datetime.now(timezone.utc),
        file_name="test_file.csv",
        file_id=uuid.uuid4(),
        file_row_count=100,
        error_row_count=0,
        staging_row_number=0,
        validating_row_number=0,
        processing_row_number=0,
        job_status="FILE_UPLOADED",
        job_type="SAMPLE",
    )

    # When
    job_dict = job.as_dict()

    # Then
    assert isinstance(job_dict, dict)
    assert job_dict["id"] == str(job.id)
    assert job_dict["collection_exercise_id"] == str(job.collection_exercise_id)
    assert job_dict["file_name"] == job.file_name
    assert job_dict["job_type"] == job.job_type
    assert job_dict["job_status"] == job.job_status
