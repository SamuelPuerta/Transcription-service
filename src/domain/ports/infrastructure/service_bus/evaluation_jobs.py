from typing import Protocol

from src.domain.entities.evaluation_job_entity import EvaluationJobEntity


class EvaluationJobsPublisher(Protocol):
    async def enqueue(self, job: EvaluationJobEntity) -> None: ...
