import os
import uuid
from datetime import datetime

import pytest
import pytz

from srm_autoprocessor.models.collection_exercise import CollectionExercise
from srm_autoprocessor.models.survey import Survey


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
def valid_collection_exercise_dict():
    return {
        "name": "example_1",
        "collection_instrument_selection_rules": ["rules"],
        "reference": "foo",
        "start_date": datetime.now().astimezone(pytz.UTC),
        "end_date": datetime.now().astimezone(pytz.UTC),
        "survey_id": "1c8f8bc8-7407-4f3e-9e39-d7ec067d10b2",
        "collection_exercise_metadata": None,
    }


@pytest.fixture
def valid_create_collection_exercise_json():
    return {
        "name": "example_1",
        "collection_instrument_selection_rules": ["rules"],
        "reference": "foo",
        "start_date": "2021-01-01T00:00:00+00:00",
        "end_date": "2021-01-02T23:59:59+00:00",
        "survey_id": "1c8f8bc8-7407-4f3e-9e39-d7ec067d10b2",
        "metadata": None,
    }


@pytest.fixture
def create_collection_exercise_dict():
    return {
        "name": "example_2",
        "collection_instrument_selection_rules": ["rules"],
        "reference": "foo",
        "start_date": "2021-01-01T00:00:00Z",
        "end_date": "2021-01-02T23:59:59Z",
        "metadata": None,
    }


@pytest.fixture
def collection_exercise_object(valid_survey_dict):
    collection_exercise_id = uuid.uuid4()
    survey_id = uuid.uuid4()
    collection_exercise = CollectionExercise(
        id=collection_exercise_id,
        name="example_1",
        reference="foo",
        start_date=datetime(2025, 1, 1, 0, 0, 0),
        end_date=datetime(2025, 1, 2, 23, 59, 59),
        survey_id=survey_id,
        collection_exercise_metadata=None,
        survey=Survey.from_dict(valid_survey_dict),
    )
    return collection_exercise
