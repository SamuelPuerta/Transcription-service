import json
from dataclasses import asdict
from azure.servicebus import ServiceBusMessage
from src.domain.entities.transcription_job_entity import TranscriptionJobEntity

def build_transcription_job_message(job: TranscriptionJobEntity) -> ServiceBusMessage:
    return ServiceBusMessage(
        body=json.dumps(asdict(job), ensure_ascii=False).encode("utf-8"),
        content_type="application/json",
        subject="transcription-job",
        message_id=job.file_id,
        correlation_id=job.correlation_id,
        application_properties={
            "batch_id": job.batch_id,
            "file_id": job.file_id,
            "initiative_id": job.initiative_id,
            "file_name": job.file_name,
            "storage_container": job.storage_container,
            "correlation_id": job.correlation_id,
        },
    )
