import json
from dataclasses import asdict
from azure.servicebus import ServiceBusMessage
from src.domain.entities.document_generation_job_entity import (
    DocumentGenerationJobEntity,
)

def build_document_generation_job_message(
    job: DocumentGenerationJobEntity,
) -> ServiceBusMessage:
    return ServiceBusMessage(
        body=json.dumps(asdict(job), ensure_ascii=False).encode("utf-8"),
        content_type="application/json",
        subject="document-generation-job",
        message_id=job.batch_id,
        correlation_id=job.correlation_id,
        application_properties={
            "batch_id": job.batch_id,
            "initiative_id": job.initiative_id,
            "correlation_id": job.correlation_id,
        },
    )
