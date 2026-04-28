import pytest
from src.domain.exceptions.base import DomainException
from src.domain.exceptions.transcription_exceptions import (
    InvalidTranscriptionJobPayload,
    UnsupportedAudioFormat,
    InvalidTranscriptionStatus,
    TranscriptionNotReady,
    TranscriptionResultMissing,
    FileAlreadyFinalized,
    MissingStorageRoutingData,
)
from src.domain.exceptions.evaluation_exceptions import (
    InvalidEvaluationJobPayload,
    MissingTranscriptionForEvaluation,
    InvalidEvaluationPrompt,
    InvalidEvaluationResponseFormat,
    EvaluationDataIncomplete,
)
from src.domain.exceptions.ingestion_exceptions import (
    InvalidStorageEvent,
    InitiativeNotFound,
    BatchNotFound,
    FileProcessingNotFound,
    DuplicateFileInBatch,
)
from src.domain.exceptions.processing_exceptions import (
    InvalidFileStatusTransition,
    InvalidBatchStatusTransition,
    BatchTotalsMismatch,
)


@pytest.mark.unit
def test_domain_exception_stores_message_code_and_extra():
    exc = DomainException("something failed", code="test_code", extra={"key": "val"})

    assert exc.message == "something failed"
    assert exc.code == "test_code"
    assert exc.extra == {"key": "val"}


@pytest.mark.unit
def test_domain_exception_str_includes_code_prefix():
    exc = DomainException("oops", code="my_code")

    assert str(exc) == "[my_code] oops"


@pytest.mark.unit
def test_domain_exception_defaults_extra_to_empty_dict():
    exc = DomainException("error")

    assert exc.extra == {}


@pytest.mark.unit
def test_domain_exception_is_exception_subclass():
    exc = DomainException("error")

    assert isinstance(exc, Exception)


# --- Transcription exceptions ---

@pytest.mark.unit
def test_invalid_transcription_job_payload_has_correct_code():
    exc = InvalidTranscriptionJobPayload("missing field_x")

    assert exc.code == "invalid_transcription_job_payload"
    assert "missing field_x" in exc.message


@pytest.mark.unit
def test_unsupported_audio_format_embeds_format_in_message():
    exc = UnsupportedAudioFormat("mp4")

    assert exc.code == "unsupported_audio_format"
    assert "mp4" in exc.message


@pytest.mark.unit
def test_transcription_not_ready_embeds_id_in_message():
    exc = TranscriptionNotReady("ts-123")

    assert exc.code == "transcription_not_ready"
    assert "ts-123" in exc.message


@pytest.mark.unit
def test_transcription_result_missing_embeds_id_in_message():
    exc = TranscriptionResultMissing("ts-456")

    assert exc.code == "transcription_result_missing"
    assert "ts-456" in exc.message


@pytest.mark.unit
def test_file_already_finalized_embeds_file_id_and_status():
    exc = FileAlreadyFinalized("f1", "completed")

    assert exc.code == "file_already_finalized"
    assert "f1" in exc.message
    assert "completed" in exc.message


@pytest.mark.unit
def test_missing_storage_routing_data_embeds_detail():
    exc = MissingStorageRoutingData("account_key missing")

    assert exc.code == "missing_storage_routing_data"
    assert "account_key missing" in exc.message


# --- Evaluation exceptions ---

@pytest.mark.unit
def test_invalid_evaluation_job_payload_has_correct_code():
    exc = InvalidEvaluationJobPayload("bad input")

    assert exc.code == "invalid_evaluation_job_payload"


@pytest.mark.unit
def test_missing_transcription_for_evaluation_embeds_file_id():
    exc = MissingTranscriptionForEvaluation("f99")

    assert exc.code == "missing_transcription_for_evaluation"
    assert "f99" in exc.message


@pytest.mark.unit
def test_invalid_evaluation_prompt_has_fixed_message():
    exc = InvalidEvaluationPrompt()

    assert exc.code == "invalid_evaluation_prompt"
    assert exc.message != ""


@pytest.mark.unit
def test_invalid_evaluation_response_format_embeds_detail():
    exc = InvalidEvaluationResponseFormat("unexpected token")

    assert exc.code == "invalid_evaluation_response_format"
    assert "unexpected token" in exc.message


# --- Ingestion exceptions ---

@pytest.mark.unit
def test_invalid_storage_event_has_correct_code():
    exc = InvalidStorageEvent("bad subject")

    assert exc.code == "invalid_storage_event"
    assert "bad subject" in exc.message


@pytest.mark.unit
def test_initiative_not_found_embeds_initiative_id():
    exc = InitiativeNotFound("PROTOCOLO_COM")

    assert exc.code == "initiative_not_found"
    assert "PROTOCOLO_COM" in exc.message


@pytest.mark.unit
def test_batch_not_found_embeds_batch_id():
    exc = BatchNotFound("INIT:2026-01-28")

    assert exc.code == "batch_not_found"
    assert "INIT:2026-01-28" in exc.message


@pytest.mark.unit
def test_file_processing_not_found_embeds_file_id():
    exc = FileProcessingNotFound("f1")

    assert exc.code == "file_processing_not_found"
    assert "f1" in exc.message


@pytest.mark.unit
def test_duplicate_file_in_batch_embeds_blob_url():
    exc = DuplicateFileInBatch("https://storage/audio.wav")

    assert exc.code == "duplicate_file_in_batch"
    assert "https://storage/audio.wav" in exc.message


# --- Processing exceptions ---

@pytest.mark.unit
def test_invalid_file_status_transition_embeds_from_and_to():
    exc = InvalidFileStatusTransition("pending", "completed")

    assert exc.code == "invalid_file_status_transition"
    assert "pending" in exc.message
    assert "completed" in exc.message


@pytest.mark.unit
def test_invalid_batch_status_transition_embeds_from_and_to():
    exc = InvalidBatchStatusTransition("processing", "pending")

    assert exc.code == "invalid_batch_status_transition"
    assert "processing" in exc.message
    assert "pending" in exc.message


@pytest.mark.unit
def test_batch_totals_mismatch_embeds_all_counts():
    exc = BatchTotalsMismatch(total=5, processed=4, completed=3, failed=2)

    assert exc.code == "batch_totals_mismatch"
    assert "5" in exc.message
    assert "4" in exc.message
    assert "3" in exc.message
    assert "2" in exc.message
