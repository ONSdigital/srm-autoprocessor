import os
import uuid
from datetime import datetime
from pathlib import Path

import pytest
import pytz

from config import config


@pytest.fixture
def valid_action_rule_dict():
    return {
        "id": uuid.uuid4(),
        "action_rule_status": "SCHEDULED",
        "classifiers": b"classifier_data",
        "created_by": "creator",
        "description": "description",
        "email_column": "email@example.com",
        "has_triggered": True,
        "phone_number_column": "1234567890",
        "selected_case_count": 10,
        "trigger_date_time": datetime.now().astimezone(pytz.UTC),
        "type": "EMAIL",
        "uac_metadata": {"key": "value"},
        "collection_exercise_id": str(uuid.uuid4()),
        "email_template_pack_code": "email_code",
        "export_file_template_pack_code": "export_code",
        "sms_template_pack_code": "sms_code",
    }


@pytest.fixture
def valid_email_action_rule_dict():
    return {
        "description": "test_action_rule",
        "trigger_date_time": datetime.now().astimezone(pytz.UTC),
        "action_rule_survey_period": "bar",
        "type": "EMAIL",
        "created_by": "dummy_user",
    }


@pytest.fixture
def valid_create_update_email_action_rule_dict():
    return {
        "description": "test_create_action_rule",
        "trigger_date_time": "2021-01-01T00:00:00+00:00",
        "type": "EMAIL",
        "created_by": "dummy_user",
    }


@pytest.fixture
def create_email_action_rule_dict():
    return {
        "description": "test_create_action_rule",
        "trigger_date_time": "2021-01-01T00:00:00Z",
        "type": "EMAIL",
        "created_by": "dummy_user",
    }


@pytest.fixture
def valid_survey_dict():
    return {
        "name": "example_1",
        "sample_definition_url": "foo",
        "sample_separator": ",",
        "sample_validation_rules": [],
        "sample_with_header_row": True,
        "metadata": None,
    }


@pytest.fixture
def survey_dict_missing_mandatory_field():
    # missing the mandatory 'name' field
    return {
        "sample_definition_url": "foo",
        "sample_separator": ",",
        "sample_validation_rules": [],
        "sample_with_header_row": True,
        "metadata": None,
    }


@pytest.fixture
def updated_survey_dict():
    return {
        "name": "updated_survey",
        "sample_definition_url": "foo",
        "sample_separator": ",",
        "sample_validation_rules": [],
        "sample_with_header_row": True,
        "metadata": None,
    }


@pytest.fixture
def change_run_mode_to_cloud():
    config.RUN_MODE = "CLOUD"
    config.SAMPLE_LOCATION = "test-bucket"


@pytest.fixture()
def create_temp_file():
    file_path = Path(__file__).parent.joinpath("test_file.csv")
    with open(file_path, "w") as job_file:
        job_file.write("header1,header2\nvalue1,value2\n")
    yield file_path
    file_path.unlink(missing_ok=True)
