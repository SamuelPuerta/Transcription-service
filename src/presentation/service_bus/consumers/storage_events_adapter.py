from azure.servicebus import ServiceBusReceivedMessage
from kink import di
from src.application.dtos.request.ingestion import StorageEventRequestDTO
from src.config.settings import settings
from src.domain.ports.application.ingestion import ProcessStorageEventUseCase
from src.infrastructure.utils.extract_blob_event_info import extract_blob_event_info
from src.presentation.service_bus.consumer import (
    BaseConsumerConfig,
    BaseServiceBusConsumer,
)

class StorageEventsConsumerAdapter(BaseServiceBusConsumer):
    def __init__(
        self,
        config: BaseConsumerConfig | None = None,
        process_storage_event_use_case: ProcessStorageEventUseCase | None = None,
    ) -> None:
        super().__init__(
            config
            or BaseConsumerConfig(
                connection_string=settings.service_bus_storage_events_connection_string,
                queue_name=settings.service_bus_storage_events_queue_name,
            )
        )
        self._process_storage_event = (
            process_storage_event_use_case or di[ProcessStorageEventUseCase]()
        )

    async def parse_message(self, msg: ServiceBusReceivedMessage) -> StorageEventRequestDTO:
        return extract_blob_event_info(msg)

    async def process(self, payload: StorageEventRequestDTO) -> None:
        await self._process_storage_event.execute(payload)
