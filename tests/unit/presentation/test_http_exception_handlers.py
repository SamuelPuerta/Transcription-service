import pytest
from unittest.mock import MagicMock
from starlette.requests import Request
from src.domain.exceptions.base import DomainException
from src.presentation.http.exception_handlers import (
    handle_domain_exception,
    handle_unhandled_exception,
    handle_validation_error,
    _status_for_domain,
)
from src.domain.exceptions.ingestion_exceptions import (
    InvalidStorageEvent,
    InitiativeNotFound,
    BatchNotFound,
    DuplicateFileInBatch,
)
from src.domain.exceptions.transcription_exceptions import (
    InvalidTranscriptionJobPayload,
    UnsupportedAudioFormat,
    TranscriptionNotReady,
    TranscriptionResultMissing,
    FileAlreadyFinalized,
    MissingStorageRoutingData,
)
from src.domain.exceptions.evaluation_exceptions import (
    InvalidEvaluationJobPayload,
    InvalidEvaluationPrompt,
    InvalidEvaluationResponseFormat,
    MissingTranscriptionForEvaluation,
    EvaluationDataIncomplete,
)
from src.domain.exceptions.processing_exceptions import (
    InvalidFileStatusTransition,
    BatchTotalsMismatch,
)


def _request(path="/test"):
    request = MagicMock(spec=Request)
    request.url.path = path
    return request


# ============================================================
# _status_for_domain
# ============================================================

@pytest.mark.unit
@pytest.mark.parametrize("exc,expected_status", [
    (InvalidStorageEvent("bad"), 400),
    (InvalidTranscriptionJobPayload("bad"), 400),
    (InvalidEvaluationJobPayload("bad"), 400),
    (InvalidEvaluationPrompt(), 400),
    (InvalidEvaluationResponseFormat("bad"), 400),
])
def test_status_for_domain_returns_400_for_invalid_payload_codes(exc, expected_status):
    assert _status_for_domain(exc) == expected_status


@pytest.mark.unit
@pytest.mark.parametrize("exc,expected_status", [
    (InitiativeNotFound("INIT"), 404),
    (BatchNotFound("b1"), 404),
])
def test_status_for_domain_returns_404_for_not_found_codes(exc, expected_status):
    assert _status_for_domain(exc) == expected_status


@pytest.mark.unit
@pytest.mark.parametrize("exc,expected_status", [
    (DuplicateFileInBatch("url"), 409),
    (InvalidFileStatusTransition("pending", "completed"), 409),
    (FileAlreadyFinalized("f1", "completed"), 409),
    (BatchTotalsMismatch(5, 4, 3, 2), 409),
])
def test_status_for_domain_returns_409_for_conflict_codes(exc, expected_status):
    assert _status_for_domain(exc) == expected_status


@pytest.mark.unit
@pytest.mark.parametrize("exc,expected_status", [
    (TranscriptionNotReady("ts-1"), 424),
    (TranscriptionResultMissing("ts-1"), 424),
    (MissingTranscriptionForEvaluation("f1"), 424),
    (EvaluationDataIncomplete("missing"), 424),
    (MissingStorageRoutingData("detail"), 424),
])
def test_status_for_domain_returns_424_for_failed_dependency_codes(exc, expected_status):
    assert _status_for_domain(exc) == expected_status


@pytest.mark.unit
def test_status_for_domain_returns_415_for_unsupported_audio_format():
    exc = UnsupportedAudioFormat("mp4")

    assert _status_for_domain(exc) == 415


@pytest.mark.unit
def test_status_for_domain_returns_400_for_unknown_code():
    exc = DomainException("unknown", code="some_unknown_code")

    assert _status_for_domain(exc) == 400


# ============================================================
# handle_domain_exception
# ============================================================

@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_domain_exception_returns_json_response_with_correct_status():
    exc = InvalidStorageEvent("bad subject")

    response = handle_domain_exception(_request(), exc)

    assert response.status_code == 400


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_domain_exception_response_body_contains_type_and_detail():
    exc = InitiativeNotFound("INIT")

    response = handle_domain_exception(_request(), exc)

    import json
    body = json.loads(response.body)
    assert body["type"] == exc.code
    assert exc.message in body["detail"]
    assert response.status_code == 404


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_domain_exception_includes_extra_when_present():
    exc = DomainException("error", code="domain_error", extra={"field": "batch_id"})

    response = handle_domain_exception(_request(), exc)

    import json
    body = json.loads(response.body)
    assert "extra" in body
    assert body["extra"]["field"] == "batch_id"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_domain_exception_omits_extra_when_empty():
    exc = DomainException("error", code="domain_error")

    response = handle_domain_exception(_request(), exc)

    import json
    body = json.loads(response.body)
    assert "extra" not in body


# ============================================================
# handle_unhandled_exception
# ============================================================

@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_unhandled_exception_returns_500():
    response = handle_unhandled_exception(_request(), RuntimeError("crash"))

    assert response.status_code == 500


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_unhandled_exception_response_body_has_internal_error_type():
    import json

    response = handle_unhandled_exception(_request(), RuntimeError("crash"))

    body = json.loads(response.body)
    assert body["type"] == "internal_error"


# ============================================================
# handle_validation_error (minimal — requires Pydantic model)
# ============================================================

@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_validation_error_returns_422():
    from pydantic import BaseModel, ValidationError

    class _M(BaseModel):
        value: int

    try:
        _M(value="not-an-int")
    except ValidationError as exc:
        response = handle_validation_error(_request(), exc)

    assert response.status_code == 422


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_validation_error_body_contains_validation_error_type():
    import json
    from pydantic import BaseModel, ValidationError

    class _M(BaseModel):
        value: int

    try:
        _M(value="not-an-int")
    except ValidationError as exc:
        response = handle_validation_error(_request(), exc)

    body = json.loads(response.body)
    assert body["type"] == "validation_error"
