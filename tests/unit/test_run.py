import csv
import logging
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

from sqlalchemy import delete
from sqlalchemy.orm import Session

from srm_autoprocessor.models import Job, JobRow
from srm_autoprocessor.run import (
    get_file_path,
    handle_file,
    process_file_with_header,
    process_job,
    staging_chunks,
    staging_job_rows,
)
from tests.unit.test_helpers import create_collection_exercise, create_job, create_survey


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
    job = MagicMock(
        file_name="test.csv", id="123e4567-e89b-12d3-a456-426614174000", staging_row_number=0, file_row_count=2
    )
    session = MagicMock(spec=Session)
    with tempfile.NamedTemporaryFile() as temp_file, patch(
        "builtins.open", mock_open(read_data="header1,header2\nvalue1,value2\n")
    ):
        job_file = Path(temp_file.name)
        result = staging_job_rows(job, job_file, session)
    assert result == "VALIDATION_IN_PROGRESS"
    session.add_all.assert_called_once()


def test_deletes_job_rows_on_validation_failure():
    job = MagicMock(
        file_name="test.csv", id="123e4567-e89b-12d3-a456-426614174000", staging_row_number=0, file_row_count=2
    )
    session = MagicMock(spec=Session)
    with tempfile.NamedTemporaryFile() as temp_file, patch(
        "builtins.open", mock_open(read_data="header1,header2\nvalue1\n")
    ):
        job_file = Path(temp_file.name)
        result = staging_job_rows(job, job_file, session)
    assert result == "VALIDATED_TOTAL_FAILURE"
    session.execute.assert_called_once()
    args, _kwargs = session.execute.call_args
    assert isinstance(args[0], delete(JobRow).__class__)
    assert str(args[0]) == str(delete(JobRow).where(JobRow.job_id == job.id))


def test_process_file_with_header():
    survey = create_survey("test_survey", None, header_row=True)
    collection_exercise = create_collection_exercise("test_collex", survey)
    job = create_job(collection_exercise, "email_driven.csv", 6, job_status="FILE_UPLOADED")
    job_file = Path(__file__).parent.parent.joinpath("resources", job.file_name)

    job_status = process_file_with_header(job, job_file)

    assert job_status == "STAGING_IN_PROGRESS", "Expected job status to be STAGING_IN_PROGRESS"


def test_process_file_with_no_sample_header():
    survey = create_survey("test_survey", None, header_row=False)
    collection_exercise = create_collection_exercise("test_collex", survey)
    job = create_job(collection_exercise, "email_driven.csv", 6, job_status="FILE_UPLOADED")
    job_file = Path(__file__).parent.parent.joinpath("resources", job.file_name)

    job_status = process_file_with_header(job, job_file)

    assert job_status == "STAGING_IN_PROGRESS", "Expected job status to be STAGING_IN_PROGRESS"


def test_process_file_with_no_job_file():
    survey = create_survey("test_survey", None, header_row=True)
    collection_exercise = create_collection_exercise("test_collex", survey)
    job = create_job(collection_exercise, "email_driven.csv", 6, job_status="FILE_UPLOADED")
    job_file = None

    job_status = process_file_with_header(job, job_file)

    assert job_status == "VALIDATED_TOTAL_FAILURE", "Expected job status to be VALIDATED_TOTAL_FAILURE"
    assert job.fatal_error_description == f"File {job.file_name} does not exist for job {job.id}"


def test_process_file_with_header_row_mismatch():
    survey_validation_rules = [
        {
            "columnName": "emailAddress",
            "rules": [{"className": "uk.gov.ons.ssdc.common.validation.EmailRule", "mandatory": True}],
            "sensitive": True,
        },
        {
            "columnName": "anotherColumn",
            "rules": [{"className": "uk.gov.ons.ssdc.common.validation.EmailRule", "mandatory": True}],
            "sensitive": True,
        },
    ]

    survey = create_survey("test_survey", survey_validation_rules, header_row=True)
    collection_exercise = create_collection_exercise("test_collex", survey)
    job = create_job(collection_exercise, "email_driven.csv", 6, job_status="FILE_UPLOADED")

    job_file = Path(__file__).parent.parent.joinpath("resources", job.file_name)

    job_status = process_file_with_header(job, job_file)

    assert job_status == "VALIDATED_TOTAL_FAILURE", "Expected job status to be VALIDATED_TOTAL_FAILURE"
    assert job.fatal_error_description == "Header row does not have expected number of columns"


def test_process_file_with_header_row_mismatch_column_name():
    survey_validation_rules = [
        {
            "columnName": "differentColumn",
            "rules": [{"className": "uk.gov.ons.ssdc.common.validation.EmailRule", "mandatory": True}],
            "sensitive": True,
        }
    ]

    survey = create_survey("test_survey", survey_validation_rules, header_row=True)
    collection_exercise = create_collection_exercise("test_collex", survey)
    job = create_job(collection_exercise, "email_driven.csv", 6, job_status="FILE_UPLOADED")

    job_file = Path(__file__).parent.parent.joinpath("resources", job.file_name)

    job_status = process_file_with_header(job, job_file)

    assert job_status == "VALIDATED_TOTAL_FAILURE", "Expected job status to be VALIDATED_TOTAL_FAILURE"


def test_staging_chunks():
    # Given

    job_file = Path(__file__).parent.parent.joinpath("resources", "email_driven.csv")
    header = ["emailAddress"]
    job = create_job(
        create_collection_exercise("test_collex", create_survey("test_survey", None)),
        "email_driven.csv",
        6,
        job_status="STAGING_IN_PROGRESS",
    )
    session = MagicMock()  # Mock session for database operations
    with open(job_file, encoding="utf-8-sig") as file:
        csvfile = csv.reader(file, delimiter=",")
        next(csvfile)  # Skip header row

        # When
        job_status = staging_chunks(csvfile, header, job, session)

    # Then
    assert job_status == "VALIDATION_IN_PROGRESS"
    assert session.add_all.call_count == 1, "Expected session.add_all to be called at least once"
    assert session.commit.call_count == 1, "Expected session.commit to be called at least once"
    assert (
        len(session.add_all.call_args.args[0]) == job.staging_row_number
    ), "Expected 5 job rows to be added to the session"


def test_staging_chunks_with_row_error():
    # Given

    job_file = Path(__file__).parent.parent.joinpath("resources", "email_driven_incorrect_line_length.csv")
    header = ["emailAddress"]
    job = create_job(
        create_collection_exercise("test_collex", create_survey("test_survey", None)),
        "email_driven_incorrect_line_length.csv",
        6,
        job_status="STAGING_IN_PROGRESS",
    )
    session = MagicMock()  # Mock session for database operations
    with open(job_file, encoding="utf-8-sig") as file:
        csvfile = csv.reader(file, delimiter=",")
        next(csvfile)  # Skip header row

        # When
        job_status = staging_chunks(csvfile, header, job, session)

    # Then
    assert job_status == "VALIDATED_TOTAL_FAILURE"
    assert session.add_all.call_count == 0, "Expected no rows to be added to the session due to error"


def test_staging_chunks_with_line_error():
    # Given

    job_file = Path(__file__).parent.parent.joinpath("resources", "email_driven_empty_chunk.csv")
    header = ["emailAddress"]
    job = create_job(
        create_collection_exercise("test_collex", create_survey("test_survey", None)),
        "email_driven_empty_chunk.csv",
        6,
        job_status="STAGING_IN_PROGRESS",
    )
    session = MagicMock()  # Mock session for database operations
    with open(job_file, encoding="utf-8-sig") as file:
        csvfile = csv.reader(file, delimiter=",")
        next(csvfile)  # Skip header row
        # When
        job_status = staging_chunks(csvfile, header, job, session)

    # Then
    assert job_status == "VALIDATED_TOTAL_FAILURE"
    assert session.add_all.call_count == 0, "Expected no rows to be added to the session due to error"
    assert job.fatal_error_description == (
        "Failed to process job due to an empty chunk, this probably indicates a "
        "mismatch between file line count and row count"
    )


def test_process_job_file_uploaded():
    # Given
    job = create_job(
        create_collection_exercise("test_collex", create_survey("test_survey", None)),
        "email_driven.csv",
        6,
        job_status="FILE_UPLOADED",
    )
    job_file = Path(__file__).parent.parent.joinpath("resources", job.file_name)

    with patch("srm_autoprocessor.run.get_file_path", return_value=job_file), patch(
        "srm_autoprocessor.run.handle_file"
    ) as mock_handle_file, patch("srm_autoprocessor.run.Session") as mock_session, patch(
        "srm_autoprocessor.run.process_file_with_header", return_value="STAGING_IN_PROGRESS"
    ) as mock_process_file_with_header:
        mock_instance = mock_session.return_value.__enter__.return_value
        mock_instance.execute.return_value.scalars.return_value.all.return_value = [job]
        # When
        process_job()

    # Then
    assert job.job_status == "STAGING_IN_PROGRESS", "Expected job status to be STAGING_IN_PROGRESS"
    mock_instance.commit.assert_called_once()
    mock_handle_file.assert_called_once_with(job_file)
    mock_process_file_with_header.assert_called_once_with(job, job_file)


def test_process_job_no_file(caplog):
    # Given
    job = create_job(
        create_collection_exercise("test_collex", create_survey("test_survey", None)),
        "email_driven.csv",
        6,
        job_status="FILE_UPLOADED",
    )
    job_file = None

    with patch("srm_autoprocessor.run.get_file_path", return_value=job_file), patch(
        "srm_autoprocessor.run.Session"
    ) as mock_session, caplog.at_level(logging.ERROR):
        mock_instance = mock_session.return_value.__enter__.return_value
        mock_instance.execute.return_value.scalars.return_value.all.return_value = [job]
        # When
        process_job()

    # Then
    assert job.job_status == "FILE_UPLOADED", "Expected job status to be FILE_UPLOADED"
    assert f"File {job.file_name} does not exist for job {job.id}" in caplog.text


def test_process_job_staging_in_progress():
    # Given
    job = create_job(
        create_collection_exercise("test_collex", create_survey("test_survey", None)),
        "email_driven.csv",
        6,
        job_status="STAGING_IN_PROGRESS",
    )
    job_file = Path(__file__).parent.parent.joinpath("resources", job.file_name)

    with patch("srm_autoprocessor.run.get_file_path", return_value=job_file), patch(
        "srm_autoprocessor.run.handle_file"
    ) as mock_handle_file, patch("srm_autoprocessor.run.Session") as mock_session, patch(
        "srm_autoprocessor.run.staging_job_rows", return_value="VALIDATION_IN_PROGRESS"
    ) as mock_staging_job_rows:
        mock_instance = mock_session.return_value.__enter__.return_value
        mock_instance.execute.return_value.scalars.return_value.all.return_value = [job]
        # When
        process_job()

    # Then
    assert job.job_status == "VALIDATION_IN_PROGRESS", "Expected job status to be STAGING_IN_PROGRESS"
    mock_instance.commit.assert_called_once()
    mock_handle_file.assert_called_once_with(job_file)
    mock_staging_job_rows.assert_called_once_with(job, job_file, mock_instance)


def test_process_job_validation_ok():
    # Given
    job = create_job(
        create_collection_exercise("test_collex", create_survey("test_survey", None)),
        "email_driven.csv",
        6,
        job_status="VALIDATED_OK",
    )
    job_file = Path(__file__).parent.parent.joinpath("resources", job.file_name)

    with patch("srm_autoprocessor.run.get_file_path", return_value=job_file), patch(
        "srm_autoprocessor.run.handle_file"
    ), patch("srm_autoprocessor.run.Session") as mock_session:
        mock_instance = mock_session.return_value.__enter__.return_value
        mock_instance.execute.return_value.scalars.return_value.all.return_value = [job]
        # When
        process_job()

    # Then
    assert job.job_status == "PROCESSING_IN_PROGRESS", "Expected job status to be STAGING_IN_PROGRESS"
    mock_instance.commit.assert_called_once()
