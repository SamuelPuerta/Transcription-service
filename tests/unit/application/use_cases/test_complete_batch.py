import pytest
from unittest.mock import AsyncMock
from src.application.use_cases.docs_gen.complete_batch import CompleteBatch
from src.application.dtos.request.docs_gen import CompleteBatchRequestDTO
from src.domain.entities.document_generation_job_entity import DocumentGenerationJobEntity
from src.domain.value_objects.call_processing_status import CallProcessingStatus


def _dto():
    return CompleteBatchRequestDTO(batch_id="INIT:2026-01-28", initiative_id="INIT", correlation_id="cid-1")


def _use_case(call_repo, doc_gen_publisher):
    return CompleteBatch(
        call_processing_repo=call_repo,
        document_generation_jobs_publisher=doc_gen_publisher,
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_enqueues_document_generation_when_batch_is_completed():
    call_repo = AsyncMock()
    call_repo.check_completion.return_value = CallProcessingStatus.COMPLETED
    doc_gen_publisher = AsyncMock()
    use_case = _use_case(call_repo, doc_gen_publisher)

    await use_case.execute(_dto())

    call_repo.check_completion.assert_awaited_once_with("INIT:2026-01-28")
    doc_gen_publisher.enqueue.assert_awaited_once()
    enqueued = doc_gen_publisher.enqueue.await_args.args[0]
    assert isinstance(enqueued, DocumentGenerationJobEntity)
    assert enqueued.batch_id == "INIT:2026-01-28"
    assert enqueued.initiative_id == "INIT"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_does_not_enqueue_when_batch_is_still_processing():
    call_repo = AsyncMock()
    call_repo.check_completion.return_value = CallProcessingStatus.PROCESSING
    doc_gen_publisher = AsyncMock()

    await _use_case(call_repo, doc_gen_publisher).execute(_dto())

    doc_gen_publisher.enqueue.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_does_not_enqueue_when_batch_ended_in_failed_state():
    call_repo = AsyncMock()
    call_repo.check_completion.return_value = CallProcessingStatus.FAILED
    doc_gen_publisher = AsyncMock()

    await _use_case(call_repo, doc_gen_publisher).execute(_dto())

    doc_gen_publisher.enqueue.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_does_not_enqueue_when_batch_is_still_pending():
    call_repo = AsyncMock()
    call_repo.check_completion.return_value = CallProcessingStatus.PENDING
    doc_gen_publisher = AsyncMock()

    await _use_case(call_repo, doc_gen_publisher).execute(_dto())

    doc_gen_publisher.enqueue.assert_not_awaited()
