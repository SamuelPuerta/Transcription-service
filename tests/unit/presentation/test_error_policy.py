import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock
from src.presentation.service_bus.error_policy import handle_message_error
from src.domain.exceptions.ingestion_exceptions import InvalidStorageEvent
from src.domain.exceptions.transcription_exceptions import (
    InvalidTranscriptionJobPayload,
    TranscriptionNotReady,
    UnsupportedAudioFormat,
)
from src.domain.exceptions.evaluation_exceptions import (
    InvalidEvaluationJobPayload,
    InvalidEvaluationPrompt,
    MissingTranscriptionForEvaluation,
)
from src.infrastructure.exceptions.base import InfrastructureException
from src.infrastructure.exceptions.service_bus_exceptions import ServiceBusMessageLockError


def _msg():
    return SimpleNamespace(message_id="M1")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_dead_letters_invalid_storage_event():
    receiver = AsyncMock()
    exc = InvalidStorageEvent("bad subject")

    await handle_message_error(receiver, _msg(), exc)

    receiver.dead_letter_message.assert_awaited_once_with(
        _msg(), reason=exc.code, error_description=exc.message
    )
    receiver.abandon_message.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_dead_letters_invalid_transcription_job_payload():
    receiver = AsyncMock()
    exc = InvalidTranscriptionJobPayload("missing field")

    await handle_message_error(receiver, _msg(), exc)

    receiver.dead_letter_message.assert_awaited_once()
    receiver.abandon_message.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_dead_letters_invalid_evaluation_job_payload():
    receiver = AsyncMock()
    exc = InvalidEvaluationJobPayload("missing batch_id")

    await handle_message_error(receiver, _msg(), exc)

    receiver.dead_letter_message.assert_awaited_once()
    receiver.abandon_message.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_dead_letters_unsupported_audio_format():
    receiver = AsyncMock()
    exc = UnsupportedAudioFormat("mp4")

    await handle_message_error(receiver, _msg(), exc)

    receiver.dead_letter_message.assert_awaited_once()
    receiver.abandon_message.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_dead_letters_invalid_evaluation_prompt():
    receiver = AsyncMock()
    exc = InvalidEvaluationPrompt()

    await handle_message_error(receiver, _msg(), exc)

    receiver.dead_letter_message.assert_awaited_once()
    receiver.abandon_message.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_abandons_retryable_domain_exception():
    receiver = AsyncMock()
    exc = TranscriptionNotReady("ts-1")

    await handle_message_error(receiver, _msg(), exc)

    receiver.abandon_message.assert_awaited_once_with(_msg())
    receiver.dead_letter_message.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_abandons_missing_transcription_for_evaluation():
    receiver = AsyncMock()
    exc = MissingTranscriptionForEvaluation("f1")

    await handle_message_error(receiver, _msg(), exc)

    receiver.abandon_message.assert_awaited_once_with(_msg())
    receiver.dead_letter_message.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_does_not_settle_when_lock_error_occurs():
    receiver = AsyncMock()
    exc = ServiceBusMessageLockError("lock expired")

    await handle_message_error(receiver, _msg(), exc)

    receiver.abandon_message.assert_not_awaited()
    receiver.dead_letter_message.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_abandons_on_infrastructure_exception():
    receiver = AsyncMock()
    exc = InfrastructureException("db connection failed", "db_error")

    await handle_message_error(receiver, _msg(), exc)

    receiver.abandon_message.assert_awaited_once_with(_msg())
    receiver.dead_letter_message.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_abandons_on_unhandled_runtime_exception():
    receiver = AsyncMock()
    exc = RuntimeError("unexpected crash")

    await handle_message_error(receiver, _msg(), exc)

    receiver.abandon_message.assert_awaited_once_with(_msg())
    receiver.dead_letter_message.assert_not_awaited()
