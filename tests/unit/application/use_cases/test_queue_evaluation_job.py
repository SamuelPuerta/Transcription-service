import pytest
from unittest.mock import AsyncMock
from src.application.use_cases.transcription.queue_evaluation_job import QueueEvaluationJob
from src.application.dtos.request.transcription import QueueEvaluationJobRequestDTO
from src.domain.entities.evaluation_job_entity import EvaluationJobEntity


@pytest.mark.unit
@pytest.mark.asyncio
async def test_enqueues_evaluation_job_entity_with_correct_fields():
    publisher = AsyncMock()
    use_case = QueueEvaluationJob(evaluation_jobs_publisher=publisher)
    dto = QueueEvaluationJobRequestDTO(file_id="f1", batch_id="INIT:2026-01-28", initiative_id="INIT", correlation_id="cid-1")

    await use_case.execute(dto)

    publisher.enqueue.assert_awaited_once()
    enqueued = publisher.enqueue.await_args.args[0]
    assert isinstance(enqueued, EvaluationJobEntity)
    assert enqueued.file_id == "f1"
    assert enqueued.batch_id == "INIT:2026-01-28"
    assert enqueued.initiative_id == "INIT"
