import pytest

from srm_autoprocessor.models.survey import Survey


def test_survey_as_dict(valid_survey_dict):
    # Given
    new_survey = Survey(
        name=valid_survey_dict["name"],
        sample_definition_url=valid_survey_dict["sample_definition_url"],
        sample_separator=valid_survey_dict["sample_separator"],
        sample_validation_rules=valid_survey_dict["sample_validation_rules"],
        sample_with_header_row=valid_survey_dict["sample_with_header_row"],
        survey_metadata=valid_survey_dict["metadata"],
    )

    # When
    new_survey_as_dict = new_survey.as_dict()

    # Then
    assert new_survey_as_dict["name"] == valid_survey_dict["name"]
    assert new_survey_as_dict["sample_definition_url"] == valid_survey_dict["sample_definition_url"]
    assert new_survey_as_dict["sample_separator"] == valid_survey_dict["sample_separator"]
    assert new_survey_as_dict["sample_validation_rules"] == valid_survey_dict["sample_validation_rules"]
    assert new_survey_as_dict["sample_with_header_row"] == valid_survey_dict["sample_with_header_row"]
    assert new_survey_as_dict["metadata"] == valid_survey_dict["metadata"]


def test_from_dict(valid_survey_dict):
    # Given
    # The survey dictionary

    # When
    created_from_dict = Survey.from_dict(valid_survey_dict)

    # Then
    assert created_from_dict.name == valid_survey_dict["name"]
    assert created_from_dict.sample_definition_url == valid_survey_dict["sample_definition_url"]
    assert created_from_dict.sample_separator == valid_survey_dict["sample_separator"]
    assert created_from_dict.sample_validation_rules == valid_survey_dict["sample_validation_rules"]
    assert created_from_dict.sample_with_header_row == valid_survey_dict["sample_with_header_row"]
    assert created_from_dict.survey_metadata == valid_survey_dict["metadata"]


def test_from_dict_missing_mandatory_field_raises_missing_key_error(survey_dict_missing_mandatory_field):
    # Given
    # The survey dictionary with a missing mandatory field

    with pytest.raises(KeyError):
        # When
        Survey.from_dict(survey_dict_missing_mandatory_field)

        # Then
        # A KeyError is raised
