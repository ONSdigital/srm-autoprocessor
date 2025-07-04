from srm_autoprocessor.models.action_rule import ActionRule


def test_action_rule_as_dict(valid_action_rule_dict):
    # Given
    new_action_rule = ActionRule(
        action_rule_status=valid_action_rule_dict["action_rule_status"],
        uac_metadata=valid_action_rule_dict["uac_metadata"],
        collection_exercise_id=valid_action_rule_dict["collection_exercise_id"],
        created_by=valid_action_rule_dict["created_by"],
        has_triggered=valid_action_rule_dict["has_triggered"],
        type=valid_action_rule_dict["type"],
        classifiers=valid_action_rule_dict["classifiers"],
        trigger_date_time=valid_action_rule_dict["trigger_date_time"],
    )

    # When
    new_action_rule_as_dict = new_action_rule.as_dict()

    # Then
    assert new_action_rule_as_dict["status"] == valid_action_rule_dict["action_rule_status"]
    assert new_action_rule_as_dict["uac_metadata"] == valid_action_rule_dict["uac_metadata"]
    assert new_action_rule_as_dict["collection_exercise_id"] == valid_action_rule_dict["collection_exercise_id"]
    assert new_action_rule_as_dict["created_by"] == valid_action_rule_dict["created_by"]
    assert new_action_rule_as_dict["has_triggered"] == valid_action_rule_dict["has_triggered"]
    assert new_action_rule_as_dict["type"] == valid_action_rule_dict["type"]
    assert new_action_rule_as_dict["classifiers"] == valid_action_rule_dict["classifiers"].decode("utf-8")
    assert new_action_rule_as_dict["trigger_date_time"] == valid_action_rule_dict["trigger_date_time"].isoformat()


def test_email_action_rule_as_dict(valid_email_action_rule_dict):
    # Given
    new_email_action_rule = ActionRule(
        description=valid_email_action_rule_dict["description"],
        type=valid_email_action_rule_dict["type"],
        trigger_date_time=valid_email_action_rule_dict["trigger_date_time"],
        created_by=valid_email_action_rule_dict["created_by"],
    )

    # When
    new_email_action_rule_as_dict = new_email_action_rule.as_dict()

    # Then
    assert new_email_action_rule_as_dict["created_by"] == valid_email_action_rule_dict["created_by"]
    assert new_email_action_rule_as_dict["description"] == valid_email_action_rule_dict["description"]
    assert new_email_action_rule_as_dict["type"] == valid_email_action_rule_dict["type"]
    assert (
        new_email_action_rule_as_dict["trigger_date_time"]
        == valid_email_action_rule_dict["trigger_date_time"].isoformat()
    )


def test_email_action_rule_from_dict(valid_create_update_email_action_rule_dict):
    # When
    new_email_action_rule = ActionRule.from_dict(valid_create_update_email_action_rule_dict)

    # Then
    assert new_email_action_rule.created_by == valid_create_update_email_action_rule_dict["created_by"]
    assert new_email_action_rule.description == valid_create_update_email_action_rule_dict["description"]
    assert (
        new_email_action_rule.trigger_date_time.isoformat()
        == valid_create_update_email_action_rule_dict["trigger_date_time"]
    )
    assert new_email_action_rule.type == valid_create_update_email_action_rule_dict["type"]
