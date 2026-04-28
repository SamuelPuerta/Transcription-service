from typing import Protocol, runtime_checkable
from src.application.dtos.request.docs_gen import CompleteBatchRequestDTO

@runtime_checkable
class CompleteBatchUseCase(Protocol):
    async def execute(self, batch: CompleteBatchRequestDTO) -> None: ...