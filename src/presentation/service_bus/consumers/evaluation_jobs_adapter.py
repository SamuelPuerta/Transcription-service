from azure.servicebus import ServiceBusReceivedMessage
from kink import di
from src.application.dtos.request.evaluation import ProcessEvaluationJobRequestDTO
from src.config.settings import settings
from src.domain.ports.application.evaluation import ProcessEvaluationJobUseCase
from src.infrastructure.utils.extract_evaluation_job import extract_evaluation_job
from src.presentation.service_bus.consumer import (
    BaseConsumerConfig,
    BaseServiceBusConsumer,
)

class EvaluationJobsConsumerAdapter(BaseServiceBusConsumer):
    def __init__(
        self,
        config: BaseConsumerConfig | None = None,
        process_evaluation_job_use_case: ProcessEvaluationJobUseCase | None = None,
    ) -> None:
        super().__init__(
            config
            or BaseConsumerConfig(
                connection_string=settings.service_bus_evaluation_jobs_connection_string,
                queue_name=settings.service_bus_evaluation_jobs_queue_name,
            )
        )
        self._process_evaluation_job = (
            process_evaluation_job_use_case or di[ProcessEvaluationJobUseCase]()
        )

    async def parse_message(self, msg: ServiceBusReceivedMessage) -> ProcessEvaluationJobRequestDTO:
        return extract_evaluation_job(msg)

    async def process(self, payload: ProcessEvaluationJobRequestDTO) -> None:
        await self._process_evaluation_job.execute(payload)
