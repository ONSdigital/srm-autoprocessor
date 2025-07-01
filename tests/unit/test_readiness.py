import contextlib
from pathlib import Path

import pytest

from srm_autoprocessor.readiness import Readiness


def test_readiness_file_is_present_while_in_context(readiness_file_path):
    with Readiness(readiness_file_path):
        assert readiness_file_path.exists(), "Readiness file not found within readiness context"


def test_readiness_file_is_not_present_after_exiting_context(readiness_file_path):
    with Readiness(readiness_file_path):
        pass
    assert not readiness_file_path.exists(), "Readiness file was still present after exiting context"


def test_readiness_file_is_not_present_after_uncaught_exception_in_context(readiness_file_path):
    with pytest.raises(Exception), Readiness(readiness_file_path):  # noqa: B017
        raise Exception
    assert not readiness_file_path.exists(), "Readiness file was still present after uncaught exception in context"


@pytest.fixture
def readiness_file_path():
    readiness_file_path = Path(__file__).parent.joinpath("test-readiness-file")
    with contextlib.suppress(FileNotFoundError):
        readiness_file_path.unlink()
    yield readiness_file_path
    with contextlib.suppress(FileNotFoundError):
        readiness_file_path.unlink()
