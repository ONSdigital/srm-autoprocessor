import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch, MagicMock, mock_open
from sqlalchemy import delete
from sqlalchemy.orm import Session

from srm_autoprocessor.models import Job, JobRow
from srm_autoprocessor.run import get_file_path, handle_file, staging_job_rows


def test_get_file_path_local():
    # Given

    job = Job(
        id=uuid.uuid4(),
        collection_exercise_id=uuid.uuid4(),
        created_at=datetime.now(timezone.utc),
        last_updated_at=datetime.now(timezone.utc),
        file_name="test_sample.csv",
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
    file_path = get_file_path(job)

    # Then

    assert file_path.name == job.file_name


def test_get_file_path_local_file_does_not_exist(caplog):
    # Given

    job = Job(
        id=uuid.uuid4(),
        collection_exercise_id=uuid.uuid4(),
        created_at=datetime.now(timezone.utc),
        last_updated_at=datetime.now(timezone.utc),
        file_name="file_not_available.csv",
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
    with caplog.at_level(logging.ERROR):
        file_path = get_file_path(job)

    # Then
    assert file_path is None, "Expected None when file does not exist"
    assert f"File {job.file_name} does not exist at path" in caplog.text


def test_get_file_path_cloud(change_run_mode_to_cloud):
    # Given

    job = Job(
        id=uuid.uuid4(),
        collection_exercise_id=uuid.uuid4(),
        created_at=datetime.now(timezone.utc),
        last_updated_at=datetime.now(timezone.utc),
        file_name="test_sample.csv",
        file_id=uuid.uuid4(),
        file_row_count=100,
        error_row_count=0,
        staging_row_number=0,
        validating_row_number=0,
        processing_row_number=0,
        job_status="FILE_UPLOADED",
        job_type="SAMPLE",
    )
    with patch("srm_autoprocessor.run.storage.Client") as mock_client:
        mock_bucket = MagicMock()
        mock_blob = MagicMock()

        mock_blob.exists.return_value = True
        mock_bucket.blob.return_value = mock_blob
        mock_client.return_value.bucket.return_value = mock_bucket
        # When
        file_path = get_file_path(job)

    # Then
    mock_client.assert_called_once()
    mock_client.return_value.bucket.assert_called_with("test-bucket")
    mock_bucket.blob.assert_called_with(job.file_name)
    mock_blob.exists.assert_called_once()
    mock_blob.download_to_filename.assert_called_once()
    assert isinstance(file_path, Path)


def test_get_file_path_cloud_file_not_exist(change_run_mode_to_cloud, caplog):
    # Given

    job = Job(
        id=uuid.uuid4(),
        collection_exercise_id=uuid.uuid4(),
        created_at=datetime.now(timezone.utc),
        last_updated_at=datetime.now(timezone.utc),
        file_name="file_not_available.csv",
        file_id=uuid.uuid4(),
        file_row_count=100,
        error_row_count=0,
        staging_row_number=0,
        validating_row_number=0,
        processing_row_number=0,
        job_status="FILE_UPLOADED",
        job_type="SAMPLE",
    )
    with caplog.at_level(logging.ERROR), patch("srm_autoprocessor.run.storage.Client") as mock_client:
        mock_bucket = MagicMock()
        mock_blob = MagicMock()

        mock_blob.exists.return_value = False
        mock_bucket.blob.return_value = mock_blob
        mock_client.return_value.bucket.return_value = mock_bucket
        # When
        file_path = get_file_path(job)

    # Then
    mock_client.assert_called_once()
    mock_client.return_value.bucket.assert_called_with("test-bucket")
    mock_bucket.blob.assert_called_with(job.file_name)
    mock_blob.exists.assert_called_once()
    assert file_path is None, "Expected None when file does not exist"
    assert f"File {job.file_name} does not exist in bucket " in caplog.text


def test_handle_path(change_run_mode_to_cloud, create_temp_file):
    # Given
    job_file = create_temp_file

    handle_file(job_file)

    # Then
    assert not job_file.exists()


def test_returns_validated_total_failure_when_file_is_none():
    job = MagicMock(file_name="test.csv", id="123e4567-e89b-12d3-a456-426614174000")
    session = MagicMock(spec=Session)
    job_file = None
    result = staging_job_rows(job, job_file, session)
    assert result == "VALIDATED_TOTAL_FAILURE"
    assert job.fatal_error_description == f"File {job.file_name} does not exist for job {job.id}"


def test_processes_file_with_valid_data():
    job = MagicMock(file_name="test.csv", id="123e4567-e89b-12d3-a456-426614174000", staging_row_number=0,
                    file_row_count=2)
    session = MagicMock(spec=Session)
    job_file = Path("/tmp/test.csv")
    with patch("builtins.open", mock_open(read_data="header1,header2\nvalue1,value2\n")):
        result = staging_job_rows(job, job_file, session)
    assert result == "VALIDATION_IN_PROGRESS"
    session.add_all.assert_called_once()


def test_deletes_job_rows_on_validation_failure():
    job = MagicMock(file_name="test.csv", id="123e4567-e89b-12d3-a456-426614174000", staging_row_number=0,
                    file_row_count=2)
    session = MagicMock(spec=Session)
    job_file = Path("/tmp/test.csv")
    with patch("builtins.open", mock_open(read_data="header1,header2\nvalue1\n")):
        result = staging_job_rows(job, job_file, session)
    assert result == "VALIDATED_TOTAL_FAILURE"
    session.execute.assert_called_once()
    args, kwargs = session.execute.call_args
    assert isinstance(args[0], delete(JobRow).__class__)
    assert str(args[0]) == str(delete(JobRow).where(JobRow.job_id == job.id))

