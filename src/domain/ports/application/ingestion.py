from typing import Protocol, runtime_checkable
from src.application.dtos.request.ingestion import StorageEventRequestDTO

@runtime_checkable
class ProcessStorageEventUseCase(Protocol):
    async def execute(self, storage_event: StorageEventRequestDTO) -> None: ...