import uuid
import pytest
from unittest.mock import AsyncMock
from src.application.dtos.request.ingestion import StorageEventRequestDTO
from src.application.use_cases.ingestion.process_storage_event import ProcessStorageEvent
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


@pytest.mark.integration
@pytest.mark.asyncio
async def test_full_ingestion_creates_batch_file_starts_transcription_and_enqueues_job():
    call_repo = AsyncMock()
    files_repo = AsyncMock()
    files_repo.create.return_value = "generated-file-id"
    publisher = AsyncMock()
    ai_gateway = AsyncMock()
    ai_gateway.create_transcription.return_value = "transcription-id-xyz"
    dto = _dto()

    use_case = ProcessStorageEvent(
        call_processing_repo=call_repo,
        files_processing_repo=files_repo,
        transcription_jobs_publisher=publisher,
        ai_gateway_adapter=ai_gateway,
    )
    await use_case.execute(dto)

    call_repo.create.assert_awaited_once()
    files_repo.create.assert_awaited_once()

    created_file: FilesProcessingEntity = files_repo.create.await_args.args[0]
    assert created_file.file_id == str(uuid.uuid5(uuid.NAMESPACE_URL, dto.blob_url))
    assert created_file.batch_id == "INIT:2026-01-28"
    assert created_file.file_name == "audio.wav"

    call_repo.increment_total_files.assert_awaited_once_with("INIT:2026-01-28")
    call_repo.mark_as_started.assert_awaited_once_with("INIT:2026-01-28")
    ai_gateway.create_transcription.assert_awaited_once_with(audio_uri=dto.blob_url)

    publisher.enqueue.assert_awaited_once()
    enqueued: TranscriptionJobEntity = publisher.enqueue.await_args.args[0]
    assert enqueued.file_id == "generated-file-id"
    assert enqueued.transcription_id == "transcription-id-xyz"
    assert enqueued.initiative_id == "INIT"
    assert enqueued.storage_container == "c1"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redelivery_reuses_existing_file_id_and_does_not_double_count():
    existing_file = FilesProcessingEntity(
        file_id="already-processed-id",
        batch_id="INIT:2026-01-28",
        file_name="audio.wav",
        blob_url="https://storage.blob.azure.com/c1/INIT/2026-01-28/audio.wav",
    )
    call_repo = AsyncMock()
    files_repo = AsyncMock()
    files_repo.create.return_value = None
    files_repo.get_by_blob_url.return_value = existing_file
    publisher = AsyncMock()
    ai_gateway = AsyncMock()
    ai_gateway.create_transcription.return_value = "ts-retry"

    use_case = ProcessStorageEvent(
        call_processing_repo=call_repo,
        files_processing_repo=files_repo,
        transcription_jobs_publisher=publisher,
        ai_gateway_adapter=ai_gateway,
    )
    await use_case.execute(_dto())

    call_repo.increment_total_files.assert_not_awaited()
    call_repo.mark_as_started.assert_awaited_once()
    publisher.enqueue.assert_awaited_once()
    enqueued: TranscriptionJobEntity = publisher.enqueue.await_args.args[0]
    assert enqueued.file_id == "already-processed-id"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_ingestion_aborts_gracefully_when_file_cannot_be_identified():
    call_repo = AsyncMock()
    files_repo = AsyncMock()
    files_repo.create.return_value = None
    files_repo.get_by_blob_url.return_value = None
    publisher = AsyncMock()
    ai_gateway = AsyncMock()

    use_case = ProcessStorageEvent(
        call_processing_repo=call_repo,
        files_processing_repo=files_repo,
        transcription_jobs_publisher=publisher,
        ai_gateway_adapter=ai_gateway,
    )
    await use_case.execute(_dto())

    ai_gateway.create_transcription.assert_not_awaited()
    publisher.enqueue.assert_not_awaited()
