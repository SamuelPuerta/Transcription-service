from src.config.settings import settings
from src.domain.entities.evaluation_job_entity import EvaluationJobEntity
from src.domain.ports.infrastructure.service_bus.evaluation_jobs import (
    EvaluationJobsPublisher,
)
from src.infrastructure.adapters.service_bus.factories.evaluation_job_message_factory import (
    build_evaluation_job_message,
)
from src.infrastructure.adapters.service_bus.publisher import (
    BasePublisherConfig,
    BaseServiceBusPublisher,
)

class EvaluationJobsPublisherAdapter(
    BaseServiceBusPublisher,
    EvaluationJobsPublisher,
):
    def __init__(
        self,
        config: BasePublisherConfig | None = None,
    ) -> None:
        super().__init__(
            config
            or BasePublisherConfig(
                connection_string=settings.service_bus_evaluation_jobs_connection_string,
                queue_name=settings.service_bus_evaluation_jobs_queue_name,
            )
        )

    async def enqueue(self, job: EvaluationJobEntity) -> None:
        await self.publish(build_evaluation_job_message(job))
