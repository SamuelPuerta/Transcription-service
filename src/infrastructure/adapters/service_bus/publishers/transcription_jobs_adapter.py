from datetime import datetime
from src.config.settings import settings
from src.domain.entities.transcription_job_entity import TranscriptionJobEntity
from src.domain.ports.infrastructure.service_bus.transcription_jobs import (
    TranscriptionJobsPublisher,
)
from src.infrastructure.adapters.service_bus.factories.transcription_job_message_factory import (
    build_transcription_job_message,
)
from src.infrastructure.adapters.service_bus.publisher import (
    BasePublisherConfig,
    BaseServiceBusPublisher,
)

class TranscriptionJobsPublisherAdapter(
    BaseServiceBusPublisher,
    TranscriptionJobsPublisher,
):
    def __init__(
        self,
        config: BasePublisherConfig | None = None,
    ) -> None:
        super().__init__(
            config
            or BasePublisherConfig(
                connection_string=settings.service_bus_transcription_jobs_connection_string,
                queue_name=settings.service_bus_transcription_jobs_queue_name,
            )
        )

    async def enqueue(
        self,
        job: TranscriptionJobEntity,
        scheduled_enqueue_time_utc: datetime | None = None,
    ) -> None:
        await self.publish(
            build_transcription_job_message(job),
            scheduled_enqueue_time_utc=scheduled_enqueue_time_utc,
        )
