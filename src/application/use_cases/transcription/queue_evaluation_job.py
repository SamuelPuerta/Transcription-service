from kink import di
from src.application.mappers.service_bus_job_mapper import to_evaluation_job_entity
from src.application.dtos.request.transcription import QueueEvaluationJobRequestDTO
from src.config.logger import logger
from src.domain.ports.application.transcription import QueueEvaluationJobUseCase
from src.domain.ports.infrastructure.service_bus.evaluation_jobs import (
    EvaluationJobsPublisher,
)

class QueueEvaluationJob(QueueEvaluationJobUseCase):
    def __init__(
        self,
        evaluation_jobs_publisher: EvaluationJobsPublisher | None = None,
    ) -> None:
        self._evaluation_jobs_publisher = (
            evaluation_jobs_publisher or di[EvaluationJobsPublisher]()
        )

    async def execute(self, evaluation: QueueEvaluationJobRequestDTO) -> None:
        log = logger.bind(correlation_id=evaluation.correlation_id)
        await self._evaluation_jobs_publisher.enqueue(to_evaluation_job_entity(evaluation))
        log.info("Trabajo de evaluacion encolado", context={
            "file_id": evaluation.file_id,
            "batch_id": evaluation.batch_id,
            "correlation_id": evaluation.correlation_id,
        })
