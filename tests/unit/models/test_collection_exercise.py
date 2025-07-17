import pytest

from srm_autoprocessor.models.collection_exercise import CollectionExercise
from srm_autoprocessor.models.survey import Survey


def test_collection_exercise_as_dict(valid_collection_exercise_dict, valid_survey_dict):
    # Given
    new_collection_exercise = CollectionExercise(
        name=valid_collection_exercise_dict["name"],
        collection_instrument_selection_rules=valid_collection_exercise_dict["collection_instrument_selection_rules"],
        reference=valid_collection_exercise_dict["reference"],
        start_date=valid_collection_exercise_dict["start_date"],
        end_date=valid_collection_exercise_dict["end_date"],
        survey_id=valid_collection_exercise_dict["survey_id"],
        collection_exercise_metadata=valid_collection_exercise_dict["collection_exercise_metadata"],
        survey=Survey.from_dict(valid_survey_dict),
    )

    # When
    new_collection_exercise_as_dict = new_collection_exercise.as_dict()

    # Then
    assert new_collection_exercise_as_dict["name"] == valid_collection_exercise_dict["name"]
    assert new_collection_exercise_as_dict["start_date"] == valid_collection_exercise_dict["start_date"].isoformat()
    assert new_collection_exercise_as_dict["end_date"] == valid_collection_exercise_dict["end_date"].isoformat()


def test_collection_exercise_from_dict(valid_create_collection_exercise_json):
    # When
    new_collection_exercise = CollectionExercise.from_dict(valid_create_collection_exercise_json)

    # Then
    assert new_collection_exercise.name == valid_create_collection_exercise_json["name"]
    assert (
        new_collection_exercise.collection_instrument_selection_rules
        == valid_create_collection_exercise_json["collection_instrument_selection_rules"]
    )
    assert new_collection_exercise.reference == valid_create_collection_exercise_json["reference"]
    assert new_collection_exercise.start_date.isoformat() == valid_create_collection_exercise_json["start_date"]
    assert new_collection_exercise.end_date.isoformat() == valid_create_collection_exercise_json["end_date"]
    assert new_collection_exercise.collection_exercise_metadata == valid_create_collection_exercise_json["metadata"]


def test_collection_exercise_from_dict_incorrect_time():
    # Given

    incorrect_collex = {
        "name": "example_1",
        "collection_instrument_selection_rules": ["rules"],
        "reference": "foo",
        "start_date": "NOT_A_TIME",
        "end_date": "2021-01-02T23:59:59+00:00",
        "survey_id": "1c8f8bc8-7407-4f3e-9e39-d7ec067d10b2",
        "metadata": None,
    }

    # When
    with pytest.raises(ValueError):
        CollectionExercise.from_dict(incorrect_collex)
