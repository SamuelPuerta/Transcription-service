import pytest
from src.domain.value_objects.files_processing_status import FilesProcessingStatus
from src.domain.value_objects.call_processing_status import CallProcessingStatus


@pytest.mark.unit
def test_files_processing_status_has_all_four_states():
    assert FilesProcessingStatus.PENDING == "pending"
    assert FilesProcessingStatus.PROCESSING == "processing"
    assert FilesProcessingStatus.COMPLETED == "completed"
    assert FilesProcessingStatus.FAILED == "failed"


@pytest.mark.unit
def test_files_processing_status_values_are_strings():
    assert isinstance(FilesProcessingStatus.PENDING, str)
    assert isinstance(FilesProcessingStatus.PROCESSING, str)
    assert isinstance(FilesProcessingStatus.COMPLETED, str)
    assert isinstance(FilesProcessingStatus.FAILED, str)


@pytest.mark.unit
def test_call_processing_status_has_all_four_states():
    assert CallProcessingStatus.PENDING == "pending"
    assert CallProcessingStatus.PROCESSING == "processing"
    assert CallProcessingStatus.COMPLETED == "completed"
    assert CallProcessingStatus.FAILED == "failed"


@pytest.mark.unit
def test_call_processing_status_values_are_strings():
    assert isinstance(CallProcessingStatus.PENDING, str)
    assert isinstance(CallProcessingStatus.PROCESSING, str)
    assert isinstance(CallProcessingStatus.COMPLETED, str)
    assert isinstance(CallProcessingStatus.FAILED, str)


@pytest.mark.unit
def test_files_and_call_status_share_same_string_values():
    assert FilesProcessingStatus.PENDING == CallProcessingStatus.PENDING
    assert FilesProcessingStatus.PROCESSING == CallProcessingStatus.PROCESSING
    assert FilesProcessingStatus.COMPLETED == CallProcessingStatus.COMPLETED
    assert FilesProcessingStatus.FAILED == CallProcessingStatus.FAILED
