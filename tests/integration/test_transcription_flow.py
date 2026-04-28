import pytest
from io import BytesIO
from datetime import datetime, timezone
from unittest.mock import AsyncMock
from src.application.use_cases.transcription.process_transcription_job import ProcessTranscriptionJob
from src.application.use_cases.transcription.enrich_file_from_manifest import EnrichFileFromManifest
from src.application.use_cases.transcription.queue_evaluation_job import QueueEvaluationJob
from src.application.dtos.request.transcription import TranscriptionJobRequestDTO
from src.domain.entities.files_processing_entity import FilesProcessingEntity
from src.domain.entities.call_processing_entity import CallProcessingEntity
from src.domain.entities.evaluation_job_entity import EvaluationJobEntity
from src.domain.entities.initiative_entity import InitiativeEntity, Storage
from src.domain.value_objects.files_processing_status import FilesProcessingStatus


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


def _initiative():
    return InitiativeEntity(
        initiative="INIT",
        name="Initiative",
        storage=Storage(accountName="acc", accountKey="key"),
    )


def _build_full_use_case(files_repo, call_repo, ai_gateway, blob_storage, initiative_repo, eval_publisher, transcription_publisher):
    queue_eval = QueueEvaluationJob(evaluation_jobs_publisher=eval_publisher)
    enrich = EnrichFileFromManifest(
        blob_storage_adapter=blob_storage,
        files_processing_repo=files_repo,
        call_processing_repo=call_repo,
        initiative_repo=initiative_repo,
    )
    return ProcessTranscriptionJob(
        files_processing_repo=files_repo,
        call_processing_repo=call_repo,
        ai_gateway_adapter=ai_gateway,
        enrich_file_from_manifest_use_case=enrich,
        queue_evaluation_job_use_case=queue_eval,
        transcription_jobs_publisher=transcription_publisher,
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_full_transcription_flow_marks_processing_sets_transcription_and_enqueues_evaluation(monkeypatch):
    files_repo = AsyncMock()
    files_repo.get_by_id.return_value = _pending_file()
    call_repo = AsyncMock()
    call_repo.get_by_id.return_value = _batch()
    ai_gateway = AsyncMock()
    ai_gateway.get_transcription_status.return_value = "Succeeded"
    ai_gateway.get_transcription_result.return_value = "This is the transcribed text"
    blob_storage = AsyncMock()
    blob_storage.download_file.return_value = BytesIO(b"")
    initiative_repo = AsyncMock()
    initiative_repo.get_by_name.return_value = _initiative()
    eval_publisher = AsyncMock()
    transcription_publisher = AsyncMock()
    monkeypatch.setattr(
        "src.application.use_cases.transcription.enrich_file_from_manifest.parse_xlsx_manifest",
        lambda _: {},
    )
    monkeypatch.setattr(
        "src.application.use_cases.transcription.enrich_file_from_manifest.norm_wav_key",
        lambda _: None,
    )

    use_case = _build_full_use_case(
        files_repo, call_repo, ai_gateway, blob_storage,
        initiative_repo, eval_publisher, transcription_publisher,
    )
    await use_case.execute(_dto())

    files_repo.mark_as_processing.assert_awaited_once_with("f1")
    ai_gateway.get_transcription_result.assert_awaited_once_with(transcription_id="ts-1")
    files_repo.set_transcription.assert_awaited_once_with("f1", "This is the transcribed text")

    eval_publisher.enqueue.assert_awaited_once()
    enqueued: EvaluationJobEntity = eval_publisher.enqueue.await_args.args[0]
    assert isinstance(enqueued, EvaluationJobEntity)
    assert enqueued.file_id == "f1"
    assert enqueued.batch_id == "INIT:2026-01-28"
    assert enqueued.initiative_id == "INIT"

    transcription_publisher.enqueue.assert_not_awaited()
    files_repo.mark_as_failed.assert_not_awaited()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_flow_reenqueues_transcription_job_when_still_running():
    files_repo = AsyncMock()
    files_repo.get_by_id.return_value = _pending_file()
    call_repo = AsyncMock()
    call_repo.get_by_id.return_value = _batch()
    ai_gateway = AsyncMock()
    ai_gateway.get_transcription_status.return_value = "Running"
    transcription_publisher = AsyncMock()
    eval_publisher = AsyncMock()

    use_case = _build_full_use_case(
        files_repo, call_repo, ai_gateway, AsyncMock(), AsyncMock(),
        eval_publisher, transcription_publisher,
    )
    await use_case.execute(_dto())

    transcription_publisher.enqueue.assert_awaited_once()
    _, kwargs = transcription_publisher.enqueue.await_args
    assert kwargs["scheduled_enqueue_time_utc"] > datetime.now(timezone.utc)
    eval_publisher.enqueue.assert_not_awaited()
    files_repo.mark_as_processing.assert_not_awaited()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_flow_marks_file_failed_and_updates_batch_on_transcription_error():
    files_repo = AsyncMock()
    files_repo.get_by_id.return_value = _pending_file()
    call_repo = AsyncMock()
    call_repo.get_by_id.return_value = _batch()
    ai_gateway = AsyncMock()
    ai_gateway.get_transcription_status.return_value = "Succeeded"
    ai_gateway.get_transcription_result.side_effect = RuntimeError("gateway unavailable")
    eval_publisher = AsyncMock()
    transcription_publisher = AsyncMock()

    use_case = _build_full_use_case(
        files_repo, call_repo, ai_gateway, AsyncMock(), AsyncMock(),
        eval_publisher, transcription_publisher,
    )

    with pytest.raises(RuntimeError, match="gateway unavailable"):
        await use_case.execute(_dto())

    files_repo.mark_as_failed.assert_awaited_once_with("f1", "gateway unavailable")
    call_repo.increment_failed.assert_awaited_once_with("INIT:2026-01-28", 1)
    call_repo.check_completion.assert_awaited_once_with("INIT:2026-01-28")
    eval_publisher.enqueue.assert_not_awaited()
