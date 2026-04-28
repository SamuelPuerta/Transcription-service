from typing import Protocol

from src.domain.entities.document_generation_job_entity import DocumentGenerationJobEntity


class DocumentGenerationJobsPublisher(Protocol):
    async def enqueue(self, job: DocumentGenerationJobEntity) -> None: ...
