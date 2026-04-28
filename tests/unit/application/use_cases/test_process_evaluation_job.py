import pytest
from unittest.mock import AsyncMock
from src.application.use_cases.evaluation.process_evaluation_job import (
    ProcessEvaluationJob,
    EVALUATION_USER_PROMPT_TEMPLATE,
)
from src.application.dtos.request.evaluation import ProcessEvaluationJobRequestDTO
from src.domain.entities.files_processing_entity import FilesProcessingEntity
from src.domain.entities.initiative_entity import InitiativeEntity, Configuration, Storage
from src.domain.value_objects.files_processing_status import FilesProcessingStatus
from src.domain.exceptions.evaluation_exceptions import InvalidEvaluationJobPayload, MissingTranscriptionForEvaluation
from src.domain.exceptions.ingestion_exceptions import FileProcessingNotFound, InitiativeNotFound


def _dto(**kw):
    base = dict(file_id="f1", batch_id="INIT:2026-01-28", initiative_id="INIT", correlation_id="cid-1")
    base.update(kw)
    return ProcessEvaluationJobRequestDTO(**base)


def _file_with_transcription(status=FilesProcessingStatus.PENDING, transcription="Hello world", consecutive="001"):
    return FilesProcessingEntity(
        file_id="f1",
        batch_id="INIT:2026-01-28",
        file_name="audio.wav",
        blob_url="https://storage/audio.wav",
        status=status,
        transcription=transcription,
        consecutive=consecutive,
    )


def _initiative():
    return InitiativeEntity(
        initiative="INIT",
        name="Initiative",
        storage=Storage(accountName="acc", accountKey="key"),
        configuration=Configuration(prompt="Evaluate the transcript."),
    )


def _use_case(files_repo, call_repo, ai_gateway, initiative_repo, finalize, complete):
    return ProcessEvaluationJob(
        ai_gateway_adapter=ai_gateway,
        files_processing_repo=files_repo,
        call_processing_repo=call_repo,
        initiative_repo=initiative_repo,
        finalize_file_evaluation_use_case=finalize,
        complete_batch_use_case=complete,
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_raises_when_file_id_is_empty():
    use_case = _use_case(AsyncMock(), AsyncMock(), AsyncMock(), AsyncMock(), AsyncMock(), AsyncMock())

    with pytest.raises(InvalidEvaluationJobPayload, match="file_id"):
        await use_case.execute(_dto(file_id=""))


@pytest.mark.unit
@pytest.mark.asyncio
async def test_raises_when_batch_id_is_empty():
    use_case = _use_case(AsyncMock(), AsyncMock(), AsyncMock(), AsyncMock(), AsyncMock(), AsyncMock())

    with pytest.raises(InvalidEvaluationJobPayload, match="batch_id"):
        await use_case.execute(_dto(batch_id=""))


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
    files_repo.get_by_id.return_value = _file_with_transcription(status=FilesProcessingStatus.COMPLETED)
    initiative_repo = AsyncMock()
    use_case = _use_case(files_repo, AsyncMock(), AsyncMock(), initiative_repo, AsyncMock(), AsyncMock())

    await use_case.execute(_dto())

    initiative_repo.get_by_name.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_returns_early_when_file_is_already_failed():
    files_repo = AsyncMock()
    files_repo.get_by_id.return_value = _file_with_transcription(status=FilesProcessingStatus.FAILED)
    initiative_repo = AsyncMock()
    use_case = _use_case(files_repo, AsyncMock(), AsyncMock(), initiative_repo, AsyncMock(), AsyncMock())

    await use_case.execute(_dto())

    initiative_repo.get_by_name.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_marks_failed_when_initiative_does_not_exist():
    files_repo = AsyncMock()
    files_repo.get_by_id.return_value = _file_with_transcription()
    initiative_repo = AsyncMock()
    initiative_repo.get_by_name.return_value = None
    call_repo = AsyncMock()
    use_case = _use_case(files_repo, call_repo, AsyncMock(), initiative_repo, AsyncMock(), AsyncMock())

    with pytest.raises(InitiativeNotFound):
        await use_case.execute(_dto())

    files_repo.mark_as_failed.assert_awaited_once()
    call_repo.increment_failed.assert_awaited_once_with("INIT:2026-01-28", 1)
    call_repo.check_completion.assert_awaited_once_with("INIT:2026-01-28")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_marks_failed_when_file_has_no_transcription():
    files_repo = AsyncMock()
    files_repo.get_by_id.return_value = _file_with_transcription(transcription="")
    initiative_repo = AsyncMock()
    initiative_repo.get_by_name.return_value = _initiative()
    call_repo = AsyncMock()
    use_case = _use_case(files_repo, call_repo, AsyncMock(), initiative_repo, AsyncMock(), AsyncMock())

    with pytest.raises(MissingTranscriptionForEvaluation):
        await use_case.execute(_dto())

    files_repo.mark_as_failed.assert_awaited_once()
    call_repo.increment_failed.assert_awaited_once_with("INIT:2026-01-28", 1)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_happy_path_calls_ai_with_correct_prompt_and_delegates_to_finalize(monkeypatch):
    files_repo = AsyncMock()
    files_repo.get_by_id.return_value = _file_with_transcription(transcription="Hello world", consecutive="001")
    initiative_repo = AsyncMock()
    initiative_repo.get_by_name.return_value = _initiative()
    ai_gateway = AsyncMock()
    ai_gateway.chat_completion.return_value = '{"score": 1}'
    finalize = AsyncMock()
    complete = AsyncMock()
    monkeypatch.setattr(
        "src.application.use_cases.evaluation.process_evaluation_job.parse_json_str",
        lambda _: {"score": 1},
    )
    use_case = _use_case(files_repo, AsyncMock(), ai_gateway, initiative_repo, finalize, complete)

    await use_case.execute(_dto())

    expected_prompt = EVALUATION_USER_PROMPT_TEMPLATE.format(transcription="Hello world", consecutive="001")
    ai_gateway.chat_completion.assert_awaited_once_with(
        system_prompt="Evaluate the transcript.",
        user_prompt=expected_prompt,
    )
    finalize.execute.assert_awaited_once()
    complete.execute.assert_awaited_once()
    files_repo.mark_as_failed.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_marks_failed_and_updates_batch_counters_on_ai_gateway_error():
    files_repo = AsyncMock()
    files_repo.get_by_id.return_value = _file_with_transcription()
    initiative_repo = AsyncMock()
    initiative_repo.get_by_name.return_value = _initiative()
    ai_gateway = AsyncMock()
    ai_gateway.chat_completion.side_effect = RuntimeError("AI timeout")
    call_repo = AsyncMock()
    use_case = _use_case(files_repo, call_repo, ai_gateway, initiative_repo, AsyncMock(), AsyncMock())

    with pytest.raises(RuntimeError, match="AI timeout"):
        await use_case.execute(_dto())

    files_repo.mark_as_failed.assert_awaited_once_with("f1", "AI timeout")
    call_repo.increment_failed.assert_awaited_once_with("INIT:2026-01-28", 1)
    call_repo.check_completion.assert_awaited_once_with("INIT:2026-01-28")
