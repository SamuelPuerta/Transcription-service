import json
from dataclasses import asdict
from azure.servicebus import ServiceBusMessage
from src.domain.entities.evaluation_job_entity import EvaluationJobEntity

def build_evaluation_job_message(job: EvaluationJobEntity) -> ServiceBusMessage:
    return ServiceBusMessage(
        body=json.dumps(asdict(job), ensure_ascii=False).encode("utf-8"),
        content_type="application/json",
        subject="evaluation-job",
        message_id=job.file_id,
        correlation_id=job.correlation_id,
        application_properties={
            "batch_id": job.batch_id,
            "file_id": job.file_id,
            "initiative_id": job.initiative_id,
            "correlation_id": job.correlation_id,
        },
    )
