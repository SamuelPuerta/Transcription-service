import json
import pytest
from types import SimpleNamespace
from src.infrastructure.utils.extract_evaluation_job import extract_evaluation_job
from src.application.dtos.request.evaluation import ProcessEvaluationJobRequestDTO
from src.domain.exceptions.evaluation_exceptions import InvalidEvaluationJobPayload


def _msg(payload: bytes):
    return SimpleNamespace(body=[payload])


def _valid_payload(**kw):
    base = dict(batch_id="INIT:2026-01-28", file_id="f1", initiative_id="INIT", correlation_id="cid-1")
    base.update(kw)
    return base


@pytest.mark.unit
def test_parses_all_fields_from_a_valid_message():
    msg = _msg(json.dumps(_valid_payload()).encode())

    result = extract_evaluation_job(msg)

    assert result.batch_id == "INIT:2026-01-28"
    assert result.file_id == "f1"
    assert result.initiative_id == "INIT"


@pytest.mark.unit
def test_raises_when_message_body_is_not_valid_json():
    msg = _msg(b"{bad json")

    with pytest.raises(InvalidEvaluationJobPayload, match="JSON"):
        extract_evaluation_job(msg)


@pytest.mark.unit
def test_raises_when_json_payload_is_a_list_instead_of_dict():
    msg = _msg(json.dumps(["not", "dict"]).encode())

    with pytest.raises(InvalidEvaluationJobPayload, match="dict"):
        extract_evaluation_job(msg)


@pytest.mark.unit
@pytest.mark.parametrize("missing_field", ["batch_id", "file_id", "initiative_id"])
def test_raises_when_required_field_is_absent(missing_field):
    payload = _valid_payload()
    del payload[missing_field]
    msg = _msg(json.dumps(payload).encode())

    with pytest.raises(InvalidEvaluationJobPayload, match="faltan"):
        extract_evaluation_job(msg)


@pytest.mark.unit
def test_raises_when_a_required_field_is_not_a_string():
    payload = _valid_payload(file_id=123)
    msg = _msg(json.dumps(payload).encode())

    with pytest.raises(InvalidEvaluationJobPayload, match="file_id"):
        extract_evaluation_job(msg)
