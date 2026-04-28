import json
import pytest
from types import SimpleNamespace
from src.infrastructure.utils.extract_transcription_job import extract_transcription_job
from src.application.dtos.request.transcription import TranscriptionJobRequestDTO
from src.domain.exceptions.transcription_exceptions import InvalidTranscriptionJobPayload


def _msg(payload: bytes):
    return SimpleNamespace(body=[payload])


def _valid_payload(**kw):
    base = dict(
        file_id="f1",
        batch_id="INIT:2026-01-28",
        initiative_id="INIT",
        transcription_id="ts-1",
        blob_url="https://storage/audio.wav",
        file_name="audio.wav",
        correlation_id="cid-1",
    )
    base.update(kw)
    return base


@pytest.mark.unit
def test_parses_all_fields_from_a_valid_message():
    msg = _msg(json.dumps(_valid_payload()).encode())

    result = extract_transcription_job(msg)

    assert result.file_id == "f1"
    assert result.batch_id == "INIT:2026-01-28"
    assert result.initiative_id == "INIT"
    assert result.transcription_id == "ts-1"
    assert result.blob_url == "https://storage/audio.wav"
    assert result.file_name == "audio.wav"


@pytest.mark.unit
def test_raises_when_message_body_is_not_valid_json():
    msg = _msg(b"{not valid json")

    with pytest.raises(InvalidTranscriptionJobPayload, match="JSON"):
        extract_transcription_job(msg)


@pytest.mark.unit
def test_raises_when_json_payload_is_a_list_instead_of_dict():
    msg = _msg(json.dumps(["a", "b"]).encode())

    with pytest.raises(InvalidTranscriptionJobPayload, match="dict"):
        extract_transcription_job(msg)


@pytest.mark.unit
@pytest.mark.parametrize(
    "missing_field",
    ["file_id", "batch_id", "initiative_id", "transcription_id", "blob_url", "file_name"],
)
def test_raises_when_required_field_is_absent(missing_field):
    payload = _valid_payload()
    del payload[missing_field]
    msg = _msg(json.dumps(payload).encode())

    with pytest.raises(InvalidTranscriptionJobPayload, match="faltan"):
        extract_transcription_job(msg)


@pytest.mark.unit
def test_raises_when_a_required_field_is_not_a_string():
    payload = _valid_payload(file_id=123)
    msg = _msg(json.dumps(payload).encode())

    with pytest.raises(InvalidTranscriptionJobPayload, match="file_id"):
        extract_transcription_job(msg)
