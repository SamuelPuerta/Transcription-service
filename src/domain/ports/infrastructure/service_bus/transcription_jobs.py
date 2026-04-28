from datetime import datetime
from typing import Protocol

from src.domain.entities.transcription_job_entity import TranscriptionJobEntity


class TranscriptionJobsPublisher(Protocol):
    async def enqueue(
        self,
        job: TranscriptionJobEntity,
        scheduled_enqueue_time_utc: datetime | None = None,
    ) -> None: ...
