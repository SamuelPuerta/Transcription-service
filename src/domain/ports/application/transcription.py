from typing import Protocol, runtime_checkable
from src.application.dtos.request.transcription import (
    ManifestEnrichmentRequestDTO,
    TranscriptionJobRequestDTO,
    QueueEvaluationJobRequestDTO
)

@runtime_checkable
class EnrichFileFromManifestUseCase(Protocol):
    async def execute(self, manifest: ManifestEnrichmentRequestDTO) -> None: ...

@runtime_checkable
class ProcessTranscriptionJobUseCase(Protocol):
    async def execute(self, transcription_job: TranscriptionJobRequestDTO) -> None: ...

@runtime_checkable
class QueueEvaluationJobUseCase(Protocol):
    async def execute(self, evaluation: QueueEvaluationJobRequestDTO) -> None: ...
