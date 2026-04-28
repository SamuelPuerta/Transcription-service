from azure.servicebus import ServiceBusReceivedMessage
from kink import di
from src.application.dtos.request.transcription import TranscriptionJobRequestDTO
from src.config.settings import settings
from src.domain.ports.application.transcription import ProcessTranscriptionJobUseCase
from src.infrastructure.utils.extract_transcription_job import extract_transcription_job
from src.presentation.service_bus.consumer import (
    BaseConsumerConfig,
    BaseServiceBusConsumer,
)

class TranscriptionJobsConsumerAdapter(BaseServiceBusConsumer):
    def __init__(
        self,
        config: BaseConsumerConfig | None = None,
        process_transcription_job_use_case: ProcessTranscriptionJobUseCase | None = None,
    ) -> None:
        super().__init__(
            config
            or BaseConsumerConfig(
                connection_string=settings.service_bus_transcription_jobs_connection_string,
                queue_name=settings.service_bus_transcription_jobs_queue_name,
            )
        )
        self._process_transcription_job = (
            process_transcription_job_use_case or di[ProcessTranscriptionJobUseCase]()
        )

    async def parse_message(self, msg: ServiceBusReceivedMessage) -> TranscriptionJobRequestDTO:
        return extract_transcription_job(msg)

    async def process(self, payload: TranscriptionJobRequestDTO) -> None:
        await self._process_transcription_job.execute(payload)
