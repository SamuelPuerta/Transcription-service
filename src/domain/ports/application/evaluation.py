from typing import Protocol, runtime_checkable
from src.application.dtos.request.evaluation import ProcessEvaluationJobRequestDTO

@runtime_checkable
class FinalizeFileEvaluationUseCase(Protocol):
    async def execute(self, evaluation: ProcessEvaluationJobRequestDTO) -> None: ...

@runtime_checkable
class ProcessEvaluationJobUseCase(Protocol):
    async def execute(self, evaluation: ProcessEvaluationJobRequestDTO) -> None: ...
