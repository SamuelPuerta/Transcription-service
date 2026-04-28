import json
import pytest
from types import SimpleNamespace
from src.infrastructure.utils.extract_blob_event_info import extract_blob_event_info
from src.application.dtos.request.ingestion import StorageEventRequestDTO
from src.domain.exceptions.ingestion_exceptions import InvalidStorageEvent


def _msg(payload: bytes):
    return SimpleNamespace(body=[payload])


def _event(subject, url="https://storage/audio.wav"):
    return json.dumps({"subject": subject, "data": {"url": url}}).encode()


@pytest.mark.unit
def test_parses_all_fields_from_a_valid_blob_created_event():
    subject = "/blobServices/default/containers/c1/blobs/INIT/2026-01-28/audio.wav"
    msg = _msg(_event(subject, url="https://storage/audio.wav"))

    result = extract_blob_event_info(msg)

    assert isinstance(result, StorageEventRequestDTO)
    assert result.container_name == "c1"
    assert result.initiative_id == "INIT"
    assert result.batch_id == "INIT:2026-01-28"
    assert result.file_name == "audio.wav"
    assert result.blob_url == "https://storage/audio.wav"


@pytest.mark.unit
def test_raises_when_subject_field_is_missing_from_event():
    msg = _msg(json.dumps({"data": {"url": "https://x"}}).encode())

    with pytest.raises(InvalidStorageEvent, match="subject"):
        extract_blob_event_info(msg)


@pytest.mark.unit
def test_raises_when_subject_does_not_start_with_blobservices_prefix():
    msg = _msg(_event("/other/prefix/containers/c1/blobs/INIT/2026-01-28/audio.wav"))

    with pytest.raises(InvalidStorageEvent, match="[Ss]ubject"):
        extract_blob_event_info(msg)


@pytest.mark.unit
def test_raises_when_blob_path_has_fewer_than_three_segments():
    msg = _msg(_event("/blobServices/default/containers/c1/blobs/INIT/2026-01-28"))

    with pytest.raises(InvalidStorageEvent):
        extract_blob_event_info(msg)


@pytest.mark.unit
def test_raises_when_file_name_is_empty_after_parsing():
    msg = _msg(_event("/blobServices/default/containers/c1/blobs/INIT/2026-01-28/"))

    with pytest.raises(InvalidStorageEvent, match="file_name"):
        extract_blob_event_info(msg)
