import pytest
from src.application.mappers.service_bus_job_mapper import (
    to_transcription_job_entity,
    to_retry_transcription_job_entity,
    to_evaluation_job_entity,
    to_document_generation_job_entity,
)
from src.application.mappers.transcription_job_mapper import (
    to_manifest_enrichment_request,
    to_queue_evaluation_job_request,
)
from src.application.mappers.evaluation_job_mapper import (
    to_finalize_file_evaluation_request,
    to_complete_batch_request,
)
from src.application.dtos.request.ingestion import StorageEventRequestDTO
from src.application.dtos.request.transcription import TranscriptionJobRequestDTO, QueueEvaluationJobRequestDTO
from src.application.dtos.request.evaluation import ProcessEvaluationJobRequestDTO
from src.application.dtos.request.docs_gen import CompleteBatchRequestDTO
from src.domain.entities.transcription_job_entity import TranscriptionJobEntity
from src.domain.entities.evaluation_job_entity import EvaluationJobEntity
from src.domain.entities.document_generation_job_entity import DocumentGenerationJobEntity


def _storage_event():
    return StorageEventRequestDTO(
        batch_id="INIT:2026-01-28",
        initiative_id="INIT",
        blob_url="https://storage/audio.wav",
        file_name="audio.wav",
        container_name="c1",
        correlation_id="cid-1",
    )


def _transcription_dto():
    return TranscriptionJobRequestDTO(
        file_id="f1",
        batch_id="INIT:2026-01-28",
        initiative_id="INIT",
        transcription_id="ts-1",
        blob_url="https://storage/audio.wav",
        file_name="audio.wav",
        correlation_id="cid-1",
    )


@pytest.mark.unit
def test_to_transcription_job_entity_maps_all_fields_from_storage_event():
    result = to_transcription_job_entity(_storage_event(), file_id="f1", transcription_id="ts-1", correlation_id="cid-1")

    assert isinstance(result, TranscriptionJobEntity)
    assert result.batch_id == "INIT:2026-01-28"
    assert result.initiative_id == "INIT"
    assert result.blob_url == "https://storage/audio.wav"
    assert result.file_name == "audio.wav"
    assert result.storage_container == "c1"
    assert result.file_id == "f1"
    assert result.transcription_id == "ts-1"


@pytest.mark.unit
def test_to_retry_transcription_job_entity_preserves_fields_and_overrides_storage_container():
    result = to_retry_transcription_job_entity(_transcription_dto(), storage_container="retry-c1")

    assert isinstance(result, TranscriptionJobEntity)
    assert result.file_id == "f1"
    assert result.batch_id == "INIT:2026-01-28"
    assert result.transcription_id == "ts-1"
    assert result.storage_container == "retry-c1"


@pytest.mark.unit
def test_to_evaluation_job_entity_maps_all_fields_from_queue_dto():
    dto = QueueEvaluationJobRequestDTO(file_id="f1", batch_id="INIT:2026-01-28", initiative_id="INIT", correlation_id="cid-1")

    result = to_evaluation_job_entity(dto)

    assert isinstance(result, EvaluationJobEntity)
    assert result.file_id == "f1"
    assert result.batch_id == "INIT:2026-01-28"
    assert result.initiative_id == "INIT"


@pytest.mark.unit
def test_to_document_generation_job_entity_maps_all_fields_from_complete_batch_dto():
    dto = CompleteBatchRequestDTO(batch_id="INIT:2026-01-28", initiative_id="INIT", correlation_id="cid-1")

    result = to_document_generation_job_entity(dto)

    assert isinstance(result, DocumentGenerationJobEntity)
    assert result.batch_id == "INIT:2026-01-28"
    assert result.initiative_id == "INIT"


@pytest.mark.unit
def test_to_manifest_enrichment_request_maps_transcription_dto_with_custom_xlsx_name():
    result = to_manifest_enrichment_request(_transcription_dto(), xlsx_name="sheet.xlsx")

    assert result.file_id == "f1"
    assert result.batch_id == "INIT:2026-01-28"
    assert result.initiative_id == "INIT"
    assert result.file_name == "audio.wav"
    assert result.xlsx_name == "sheet.xlsx"


@pytest.mark.unit
def test_to_queue_evaluation_job_request_maps_all_fields_from_transcription_dto():
    result = to_queue_evaluation_job_request(_transcription_dto())

    assert isinstance(result, QueueEvaluationJobRequestDTO)
    assert result.file_id == "f1"
    assert result.batch_id == "INIT:2026-01-28"
    assert result.initiative_id == "INIT"


@pytest.mark.unit
def test_to_finalize_file_evaluation_request_copies_dto_and_attaches_evaluation_json():
    dto = ProcessEvaluationJobRequestDTO(file_id="f1", batch_id="INIT:2026-01-28", initiative_id="INIT", correlation_id="cid-1")
    evaluation_json = {"score": 10, "observations": "Good"}

    result = to_finalize_file_evaluation_request(dto, evaluation_json=evaluation_json)

    assert isinstance(result, ProcessEvaluationJobRequestDTO)
    assert result.file_id == "f1"
    assert result.batch_id == "INIT:2026-01-28"
    assert result.initiative_id == "INIT"
    assert result.evaluation_json == {"score": 10, "observations": "Good"}


@pytest.mark.unit
def test_to_complete_batch_request_maps_batch_and_initiative_from_evaluation_dto():
    dto = ProcessEvaluationJobRequestDTO(file_id="f1", batch_id="INIT:2026-01-28", initiative_id="INIT", correlation_id="cid-1")

    result = to_complete_batch_request(dto)

    assert isinstance(result, CompleteBatchRequestDTO)
    assert result.batch_id == "INIT:2026-01-28"
    assert result.initiative_id == "INIT"
