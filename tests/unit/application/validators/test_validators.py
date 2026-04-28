import pytest
from src.application.validators.transcription_job import validate_job as validate_transcription_job
from src.application.validators.evaluation_job import validate_job as validate_evaluation_job
from src.application.dtos.request.transcription import TranscriptionJobRequestDTO
from src.application.dtos.request.evaluation import ProcessEvaluationJobRequestDTO
from src.domain.exceptions.transcription_exceptions import InvalidTranscriptionJobPayload
from src.domain.exceptions.evaluation_exceptions import InvalidEvaluationJobPayload


def _transcription_dto(**kw):
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
    return TranscriptionJobRequestDTO(**base)


def _evaluation_dto(**kw):
    base = dict(file_id="f1", batch_id="INIT:2026-01-28", initiative_id="INIT", correlation_id="cid-1")
    base.update(kw)
    return ProcessEvaluationJobRequestDTO(**base)


@pytest.mark.unit
@pytest.mark.parametrize(
    "field",
    ["file_id", "batch_id", "initiative_id", "transcription_id", "blob_url", "file_name"],
)
def test_validate_transcription_job_raises_when_required_field_is_empty(field):
    dto = _transcription_dto(**{field: ""})

    with pytest.raises(InvalidTranscriptionJobPayload, match=field):
        validate_transcription_job(dto)


@pytest.mark.unit
def test_validate_transcription_job_passes_when_all_fields_are_present():
    validate_transcription_job(_transcription_dto())


@pytest.mark.unit
@pytest.mark.parametrize("field", ["file_id", "batch_id", "initiative_id"])
def test_validate_evaluation_job_raises_when_required_field_is_empty(field):
    dto = _evaluation_dto(**{field: ""})

    with pytest.raises(InvalidEvaluationJobPayload, match=field):
        validate_evaluation_job(dto)


@pytest.mark.unit
def test_validate_evaluation_job_passes_when_all_fields_are_present():
    validate_evaluation_job(_evaluation_dto())
