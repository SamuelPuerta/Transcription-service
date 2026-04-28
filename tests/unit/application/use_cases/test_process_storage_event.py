import uuid
import pytest
from unittest.mock import AsyncMock
from src.application.dtos.request.ingestion import StorageEventRequestDTO
from src.application.use_cases.ingestion.process_storage_event import ProcessStorageEvent
from src.domain.entities.call_processing_entity import CallProcessingEntity
from src.domain.entities.files_processing_entity import FilesProcessingEntity
from src.domain.entities.transcription_job_entity import TranscriptionJobEntity


def _dto(**kw):
    base = dict(
        batch_id="INIT:2026-01-28",
        initiative_id="INIT",
        blob_url="https://storage.blob.azure.com/c1/INIT/2026-01-28/audio.wav",
        file_name="audio.wav",
        container_name="c1",
        correlation_id="cid-1",
    )
    base.update(kw)
    return StorageEventRequestDTO(**base)


def _use_case(call_repo, files_repo, publisher, ai_gateway):
    return ProcessStorageEvent(
        call_processing_repo=call_repo,
        files_processing_repo=files_repo,
        transcription_jobs_publisher=publisher,
        ai_gateway_adapter=ai_gateway,
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_execute_creates_batch_and_file_and_enqueues_transcription_job():
    call_repo = AsyncMock()
    files_repo = AsyncMock()
    files_repo.create.return_value = "file-id-1"
    publisher = AsyncMock()
    ai_gateway = AsyncMock()
    ai_gateway.create_transcription.return_value = "ts-123"
    dto = _dto()

    await _use_case(call_repo, files_repo, publisher, ai_gateway).execute(dto)

    created_call = call_repo.create.await_args.args[0]
    assert isinstance(created_call, CallProcessingEntity)
    assert created_call.batch_id == "INIT:2026-01-28"
    assert created_call.initiative_id == "INIT"
    assert created_call.storage_container == "c1"

    created_file = files_repo.create.await_args.args[0]
    assert isinstance(created_file, FilesProcessingEntity)
    assert created_file.file_id == str(uuid.uuid5(uuid.NAMESPACE_URL, dto.blob_url))
    assert created_file.batch_id == "INIT:2026-01-28"
    assert created_file.file_name == "audio.wav"
    assert created_file.xlsx_name == "Copia_CSV.xlsx"

    call_repo.increment_total_files.assert_awaited_once_with("INIT:2026-01-28")
    call_repo.mark_as_started.assert_awaited_once_with("INIT:2026-01-28")
    ai_gateway.create_transcription.assert_awaited_once_with(audio_uri=dto.blob_url)

    enqueued = publisher.enqueue.await_args.args[0]
    assert isinstance(enqueued, TranscriptionJobEntity)
    assert enqueued.file_id == "file-id-1"
    assert enqueued.transcription_id == "ts-123"
    assert enqueued.batch_id == "INIT:2026-01-28"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_execute_on_redelivery_uses_existing_file_without_incrementing_total():
    existing = FilesProcessingEntity(
        file_id="existing-id",
        batch_id="INIT:2026-01-28",
        file_name="audio.wav",
        blob_url="https://storage.blob.azure.com/c1/INIT/2026-01-28/audio.wav",
    )
    call_repo = AsyncMock()
    files_repo = AsyncMock()
    files_repo.create.return_value = None
    files_repo.get_by_blob_url.return_value = existing
    publisher = AsyncMock()
    ai_gateway = AsyncMock()
    ai_gateway.create_transcription.return_value = "ts-999"

    await _use_case(call_repo, files_repo, publisher, ai_gateway).execute(_dto())

    files_repo.get_by_blob_url.assert_awaited_once()
    call_repo.increment_total_files.assert_not_awaited()
    call_repo.mark_as_started.assert_awaited_once()
    enqueued = publisher.enqueue.await_args.args[0]
    assert enqueued.file_id == "existing-id"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_execute_returns_early_when_no_file_id_and_no_existing_file():
    call_repo = AsyncMock()
    files_repo = AsyncMock()
    files_repo.create.return_value = None
    files_repo.get_by_blob_url.return_value = None
    publisher = AsyncMock()
    ai_gateway = AsyncMock()

    await _use_case(call_repo, files_repo, publisher, ai_gateway).execute(_dto())

    call_repo.mark_as_started.assert_not_awaited()
    ai_gateway.create_transcription.assert_not_awaited()
    publisher.enqueue.assert_not_awaited()
