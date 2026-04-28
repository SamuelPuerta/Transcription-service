from src.config.settings import settings
from src.domain.entities.document_generation_job_entity import (
    DocumentGenerationJobEntity,
)
from src.domain.ports.infrastructure.service_bus.document_generation_jobs import (
    DocumentGenerationJobsPublisher,
)
from src.infrastructure.adapters.service_bus.factories.document_generation_job_message_factory import (
    build_document_generation_job_message,
)
from src.infrastructure.adapters.service_bus.publisher import (
    BasePublisherConfig,
    BaseServiceBusPublisher,
)

class DocumentGenerationJobsPublisherAdapter(
    BaseServiceBusPublisher,
    DocumentGenerationJobsPublisher,
):
    def __init__(
        self,
        config: BasePublisherConfig | None = None,
    ) -> None:
        super().__init__(
            config
            or BasePublisherConfig(
                connection_string=settings.service_bus_document_generation_jobs_connection_string,
                queue_name=settings.service_bus_document_generation_jobs_queue_name,
            )
        )

    async def enqueue(self, job: DocumentGenerationJobEntity) -> None:
        await self.publish(build_document_generation_job_message(job))
