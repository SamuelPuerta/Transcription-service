import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock
from src.application.use_cases.transcription.process_transcription_job import ProcessTranscriptionJob
from src.application.dtos.request.transcription import TranscriptionJobRequestDTO
from src.domain.entities.files_processing_entity import FilesProcessingEntity
from src.domain.entities.call_processing_entity import CallProcessingEntity
from src.domain.value_objects.files_processing_status import FilesProcessingStatus
from src.domain.exceptions.transcription_exceptions import InvalidTranscriptionJobPayload
from src.domain.exceptions.ingestion_exceptions import BatchNotFound, FileProcessingNotFound


def _dto(**kw):
    base = dict(
        file_id="f1",
        batch_id="INIT:2026-01-28",
        initiative_id="INIT",
        transcription_id="ts-1",
        blob_url="https://storage/audio.wav",
        file_name="audio.wav",
        correlation_id="cid-1",
    )
    base.update(kw)
    return TranscriptionJobRequestDTO(**base)


def _pending_file():
    return FilesProcessingEntity(
        file_id="f1",
        batch_id="INIT:2026-01-28",
        file_name="audio.wav",
        blob_url="https://storage/audio.wav",
        status=FilesProcessingStatus.PENDING,
        xlsx_name="Copia_CSV.xlsx",
    )


def _batch():
    return CallProcessingEntity(
        batch_id="INIT:2026-01-28",
        initiative_id="INIT",
        storage_container="c1",
    )


def _use_case(files_repo, call_repo, ai_gateway, enrich, queue_eval, publisher):
    return ProcessTranscriptionJob(
        files_processing_repo=files_repo,
        call_processing_repo=call_repo,
        ai_gateway_adapter=ai_gateway,
        enrich_file_from_manifest_use_case=enrich,
        queue_evaluation_job_use_case=queue_eval,
        transcription_jobs_publisher=publisher,
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_raises_when_file_id_is_empty():
    use_case = _use_case(AsyncMock(), AsyncMock(), AsyncMock(), AsyncMock(), AsyncMock(), AsyncMock())

    with pytest.raises(InvalidTranscriptionJobPayload, match="file_id"):
        await use_case.execute(_dto(file_id=""))


@pytest.mark.unit
@pytest.mark.asyncio
async def test_raises_when_transcription_id_is_empty():
    use_case = _use_case(AsyncMock(), AsyncMock(), AsyncMock(), AsyncMock(), AsyncMock(), AsyncMock())

    with pytest.raises(InvalidTranscriptionJobPayload, match="transcription_id"):
        await use_case.execute(_dto(transcription_id=""))


@pytest.mark.unit
@pytest.mark.asyncio
async def test_raises_when_file_entity_does_not_exist():
    files_repo = AsyncMock()
    files_repo.get_by_id.return_value = None
    use_case = _use_case(files_repo, AsyncMock(), AsyncMock(), AsyncMock(), AsyncMock(), AsyncMock())

    with pytest.raises(FileProcessingNotFound):
        await use_case.execute(_dto())


@pytest.mark.unit
@pytest.mark.asyncio
async def test_returns_early_when_file_is_already_completed():
    files_repo = AsyncMock()
    files_repo.get_by_id.return_value = FilesProcessingEntity(
        file_id="f1", batch_id="INIT:2026-01-28", file_name="audio.wav",
        blob_url="https://storage/audio.wav", status=FilesProcessingStatus.COMPLETED,
    )
    call_repo = AsyncMock()
    ai_gateway = AsyncMock()
    use_case = _use_case(files_repo, call_repo, ai_gateway, AsyncMock(), AsyncMock(), AsyncMock())

    await use_case.execute(_dto())

    call_repo.get_by_id.assert_not_awaited()
    ai_gateway.get_transcription_status.assert_not_awaited()
    files_repo.mark_as_processing.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_returns_early_when_file_is_already_failed():
    files_repo = AsyncMock()
    files_repo.get_by_id.return_value = FilesProcessingEntity(
        file_id="f1", batch_id="INIT:2026-01-28", file_name="audio.wav",
        blob_url="https://storage/audio.wav", status=FilesProcessingStatus.FAILED,
    )
    call_repo = AsyncMock()
    use_case = _use_case(files_repo, call_repo, AsyncMock(), AsyncMock(), AsyncMock(), AsyncMock())

    await use_case.execute(_dto())

    call_repo.get_by_id.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_raises_when_batch_entity_does_not_exist():
    files_repo = AsyncMock()
    files_repo.get_by_id.return_value = _pending_file()
    call_repo = AsyncMock()
    call_repo.get_by_id.return_value = None
    use_case = _use_case(files_repo, call_repo, AsyncMock(), AsyncMock(), AsyncMock(), AsyncMock())

    with pytest.raises(BatchNotFound):
        await use_case.execute(_dto())


@pytest.mark.unit
@pytest.mark.asyncio
async def test_reenqueues_with_delay_when_transcription_not_started():
    files_repo = AsyncMock()
    files_repo.get_by_id.return_value = _pending_file()
    call_repo = AsyncMock()
    call_repo.get_by_id.return_value = _batch()
    ai_gateway = AsyncMock()
    ai_gateway.get_transcription_status.return_value = "NotStarted"
    publisher = AsyncMock()
    use_case = _use_case(files_repo, call_repo, ai_gateway, AsyncMock(), AsyncMock(), publisher)

    await use_case.execute(_dto())

    publisher.enqueue.assert_awaited_once()
    _, kwargs = publisher.enqueue.await_args
    assert "scheduled_enqueue_time_utc" in kwargs
    assert kwargs["scheduled_enqueue_time_utc"] > datetime.now(timezone.utc)
    files_repo.mark_as_processing.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_reenqueues_with_delay_when_transcription_still_running():
    files_repo = AsyncMock()
    files_repo.get_by_id.return_value = _pending_file()
    call_repo = AsyncMock()
    call_repo.get_by_id.return_value = _batch()
    ai_gateway = AsyncMock()
    ai_gateway.get_transcription_status.return_value = "Running"
    publisher = AsyncMock()
    use_case = _use_case(files_repo, call_repo, ai_gateway, AsyncMock(), AsyncMock(), publisher)

    await use_case.execute(_dto())

    publisher.enqueue.assert_awaited_once()
    files_repo.mark_as_processing.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_happy_path_marks_processing_enriches_sets_transcription_and_queues_evaluation():
    files_repo = AsyncMock()
    files_repo.get_by_id.return_value = _pending_file()
    call_repo = AsyncMock()
    call_repo.get_by_id.return_value = _batch()
    ai_gateway = AsyncMock()
    ai_gateway.get_transcription_status.return_value = "Succeeded"
    ai_gateway.get_transcription_result.return_value = "Hello world transcription"
    enrich = AsyncMock()
    queue_eval = AsyncMock()
    use_case = _use_case(files_repo, call_repo, ai_gateway, enrich, queue_eval, AsyncMock())

    await use_case.execute(_dto())

    files_repo.mark_as_processing.assert_awaited_once_with("f1")
    ai_gateway.get_transcription_result.assert_awaited_once_with(transcription_id="ts-1")
    enrich.execute.assert_awaited_once()
    files_repo.set_transcription.assert_awaited_once_with("f1", "Hello world transcription")
    queue_eval.execute.assert_awaited_once()
    files_repo.mark_as_failed.assert_not_awaited()
    call_repo.increment_failed.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_marks_file_failed_and_updates_batch_counters_on_error():
    files_repo = AsyncMock()
    files_repo.get_by_id.return_value = _pending_file()
    call_repo = AsyncMock()
    call_repo.get_by_id.return_value = _batch()
    ai_gateway = AsyncMock()
    ai_gateway.get_transcription_status.return_value = "Succeeded"
    ai_gateway.get_transcription_result.side_effect = RuntimeError("gateway failure")
    use_case = _use_case(files_repo, call_repo, ai_gateway, AsyncMock(), AsyncMock(), AsyncMock())

    with pytest.raises(RuntimeError, match="gateway failure"):
        await use_case.execute(_dto())

    files_repo.mark_as_failed.assert_awaited_once_with("f1", "gateway failure")
    call_repo.increment_failed.assert_awaited_once_with("INIT:2026-01-28", 1)
    call_repo.check_completion.assert_awaited_once_with("INIT:2026-01-28")
