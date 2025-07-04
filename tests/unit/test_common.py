from srm_autoprocessor.common.strtobool import strtobool


def test_strtobool_returns_true_for_valid_true_values():
    assert strtobool("true") is True
    assert strtobool("yes") is True
    assert strtobool("y") is True
    assert strtobool("on") is True
    assert strtobool("1") is True


def test_strtobool_returns_false_for_valid_false_values():
    assert strtobool("false") is False
    assert strtobool("no") is False
    assert strtobool("n") is False
    assert strtobool("off") is False
    assert strtobool("0") is False


def test_strtobool_raises_value_error_for_invalid_values():
    try:
        strtobool("invalid")
    except ValueError:
        pass
    else:
        raise AssertionError("Expected ValueError for invalid input")


def test_strtobool_is_case_insensitive():
    assert strtobool("TRUE") is True
    assert strtobool("False") is False
    assert strtobool("YeS") is True
    assert strtobool("nO") is False
