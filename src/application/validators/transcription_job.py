from src.application.dtos.request.transcription import TranscriptionJobRequestDTO
from src.domain.exceptions.transcription_exceptions import InvalidTranscriptionJobPayload

def validate_job(transcription_job: TranscriptionJobRequestDTO) -> None:
        if not transcription_job.file_id:
            raise InvalidTranscriptionJobPayload("job.file_id es requerido")
        if not transcription_job.batch_id:
            raise InvalidTranscriptionJobPayload("job.batch_id es requerido")
        if not transcription_job.initiative_id:
            raise InvalidTranscriptionJobPayload("job.initiative_id es requerido")
        if not transcription_job.transcription_id:
            raise InvalidTranscriptionJobPayload("job.transcription_id es requerido")
        if not transcription_job.blob_url:
            raise InvalidTranscriptionJobPayload("job.blob_url es requerido")
        if not transcription_job.file_name:
            raise InvalidTranscriptionJobPayload("job.file_name es requerido")
        if not transcription_job.correlation_id:
            raise InvalidTranscriptionJobPayload("job.correlation_id es requerido")