from src.application.dtos.request.transcription import (
    ManifestEnrichmentRequestDTO,
    QueueEvaluationJobRequestDTO,
    TranscriptionJobRequestDTO,
)

def to_manifest_enrichment_request(
    transcription_job: TranscriptionJobRequestDTO,
    *,
    xlsx_name: str,
) -> ManifestEnrichmentRequestDTO:
    return ManifestEnrichmentRequestDTO(
        file_id=transcription_job.file_id,
        batch_id=transcription_job.batch_id,
        initiative_id=transcription_job.initiative_id,
        file_name=transcription_job.file_name,
        xlsx_name=xlsx_name,
    )

def to_queue_evaluation_job_request(
    transcription_job: TranscriptionJobRequestDTO,
) -> QueueEvaluationJobRequestDTO:
    return QueueEvaluationJobRequestDTO(
        file_id=transcription_job.file_id,
        batch_id=transcription_job.batch_id,
        initiative_id=transcription_job.initiative_id,
        correlation_id=transcription_job.correlation_id,
    )
