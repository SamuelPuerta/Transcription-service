from src.application.dtos.request.docs_gen import CompleteBatchRequestDTO
from src.application.dtos.request.ingestion import StorageEventRequestDTO
from src.application.dtos.request.transcription import (
    QueueEvaluationJobRequestDTO,
    TranscriptionJobRequestDTO,
)
from src.domain.entities.document_generation_job_entity import (
    DocumentGenerationJobEntity,
)
from src.domain.entities.evaluation_job_entity import EvaluationJobEntity
from src.domain.entities.transcription_job_entity import TranscriptionJobEntity

def to_transcription_job_entity(
    storage_event: StorageEventRequestDTO,
    *,
    file_id: str,
    transcription_id: str,
    correlation_id: str,
) -> TranscriptionJobEntity:
    return TranscriptionJobEntity(
        batch_id=storage_event.batch_id,
        blob_url=storage_event.blob_url,
        file_name=storage_event.file_name,
        file_id=file_id,
        initiative_id=storage_event.initiative_id,
        storage_container=storage_event.container_name,
        transcription_id=transcription_id,
        correlation_id=correlation_id,
    )

def to_retry_transcription_job_entity(
    transcription_job: TranscriptionJobRequestDTO,
    *,
    storage_container: str,
) -> TranscriptionJobEntity:
    return TranscriptionJobEntity(
        batch_id=transcription_job.batch_id,
        blob_url=transcription_job.blob_url,
        file_name=transcription_job.file_name,
        file_id=transcription_job.file_id,
        initiative_id=transcription_job.initiative_id,
        storage_container=storage_container,
        transcription_id=transcription_job.transcription_id,
        correlation_id=transcription_job.correlation_id,
    )

def to_evaluation_job_entity(
    evaluation: QueueEvaluationJobRequestDTO,
) -> EvaluationJobEntity:
    return EvaluationJobEntity(
        batch_id=evaluation.batch_id,
        file_id=evaluation.file_id,
        initiative_id=evaluation.initiative_id,
        correlation_id=evaluation.correlation_id,
    )

def to_document_generation_job_entity(
    document: CompleteBatchRequestDTO,
) -> DocumentGenerationJobEntity:
    return DocumentGenerationJobEntity(
        batch_id=document.batch_id,
        initiative_id=document.initiative_id,
        correlation_id=document.correlation_id,
    )
