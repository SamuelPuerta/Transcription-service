import asyncio
import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
import src.presentation.service_bus.consumer as consumer_mod
from src.presentation.service_bus.consumer import BaseConsumerConfig
from src.presentation.service_bus.consumers.storage_events_adapter import StorageEventsConsumerAdapter
from src.presentation.service_bus.consumers.transcription_jobs_adapter import TranscriptionJobsConsumerAdapter
from src.presentation.service_bus.consumers.evaluation_jobs_adapter import EvaluationJobsConsumerAdapter
from src.application.dtos.request.ingestion import StorageEventRequestDTO
from src.application.dtos.request.transcription import TranscriptionJobRequestDTO
from src.application.dtos.request.evaluation import ProcessEvaluationJobRequestDTO
from src.domain.exceptions.ingestion_exceptions import InvalidStorageEvent
from src.domain.exceptions.transcription_exceptions import InvalidTranscriptionJobPayload
from src.domain.exceptions.evaluation_exceptions import InvalidEvaluationJobPayload
from src.infrastructure.exceptions.service_bus_exceptions import ServiceBusConsumeError


class _FakeRenewer:
    def __init__(self):
        self.register_calls = []

    def register(self, receiver, msg, max_lock_renewal_duration=None):
        self.register_calls.append(msg)

    async def close(self):
        pass


class _FakeReceiver:
    def __init__(self, batches=None):
        self._batches = list(batches or [])
        self.complete_message = AsyncMock()
        self.dead_letter_message = AsyncMock()
        self.abandon_message = AsyncMock()

    async def receive_messages(self, max_wait_time=None, max_message_count=None):
        return self._batches.pop(0) if self._batches else []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False


class _FakeClient:
    def __init__(self, receiver):
        self._receiver = receiver
        self.queue_names = []

    def get_queue_receiver(self, name):
        self.queue_names.append(name)
        return self._receiver

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False


class _FakeTask:
    def __init__(self, done=False, await_raises_cancelled=False):
        self._done = done
        self._cancelled = False
        self._await_raises_cancelled = await_raises_cancelled

    def done(self):
        return self._done

    def cancel(self):
        self._cancelled = True

    def __await__(self):
        async def _w():
            if self._await_raises_cancelled:
                raise asyncio.CancelledError()
        return _w().__await__()


def _renewer_factory(monkeypatch):
    renewer = _FakeRenewer()
    monkeypatch.setattr(consumer_mod, "AutoLockRenewer", lambda: renewer)
    return renewer


def _storage_adapter(monkeypatch, use_case=None):
    _renewer_factory(monkeypatch)
    uc = use_case or AsyncMock()
    adapter = StorageEventsConsumerAdapter(
        config=BaseConsumerConfig(connection_string="cs", queue_name="q-storage"),
        process_storage_event_use_case=uc,
    )
    return adapter, uc


def _transcription_adapter(monkeypatch, use_case=None):
    _renewer_factory(monkeypatch)
    uc = use_case or AsyncMock()
    adapter = TranscriptionJobsConsumerAdapter(
        config=BaseConsumerConfig(connection_string="cs", queue_name="q-transcription"),
        process_transcription_job_use_case=uc,
    )
    return adapter, uc


def _evaluation_adapter(monkeypatch, use_case=None):
    _renewer_factory(monkeypatch)
    uc = use_case or AsyncMock()
    adapter = EvaluationJobsConsumerAdapter(
        config=BaseConsumerConfig(connection_string="cs", queue_name="q-evaluation"),
        process_evaluation_job_use_case=uc,
    )
    return adapter, uc


# ============================================================
# StorageEventsConsumerAdapter
# ============================================================

@pytest.mark.unit
@pytest.mark.asyncio
async def test_storage_parse_message_returns_dto_on_success(monkeypatch):
    adapter, _ = _storage_adapter(monkeypatch)
    expected = StorageEventRequestDTO(
        batch_id="INIT:2026-01-28", initiative_id="INIT",
        blob_url="https://x", file_name="audio.wav", container_name="c1",
        correlation_id="cid-1",
    )
    monkeypatch.setattr(
        "src.presentation.service_bus.consumers.storage_events_adapter.extract_blob_event_info",
        lambda _: expected,
    )

    result = await adapter.parse_message(SimpleNamespace(message_id="M1"))

    assert result == expected


@pytest.mark.unit
@pytest.mark.asyncio
async def test_storage_parse_message_propagates_invalid_storage_event(monkeypatch):
    adapter, _ = _storage_adapter(monkeypatch)
    monkeypatch.setattr(
        "src.presentation.service_bus.consumers.storage_events_adapter.extract_blob_event_info",
        lambda _: (_ for _ in ()).throw(InvalidStorageEvent("bad")),
    )

    with pytest.raises(InvalidStorageEvent):
        await adapter.parse_message(SimpleNamespace(message_id="M1"))


@pytest.mark.unit
@pytest.mark.asyncio
async def test_storage_process_delegates_to_use_case(monkeypatch):
    adapter, use_case = _storage_adapter(monkeypatch)
    dto = StorageEventRequestDTO(
        batch_id="INIT:2026-01-28", initiative_id="INIT",
        blob_url="https://x", file_name="audio.wav", container_name="c1",
        correlation_id="cid-1",
    )

    await adapter.process(dto)

    use_case.execute.assert_awaited_once_with(dto)


# ============================================================
# TranscriptionJobsConsumerAdapter
# ============================================================

@pytest.mark.unit
@pytest.mark.asyncio
async def test_transcription_parse_message_returns_dto_on_success(monkeypatch):
    adapter, _ = _transcription_adapter(monkeypatch)
    expected = TranscriptionJobRequestDTO(
        file_id="f1", batch_id="INIT:2026-01-28", initiative_id="INIT",
        transcription_id="ts-1", blob_url="https://x", file_name="audio.wav",
        correlation_id="cid-1",
    )
    monkeypatch.setattr(
        "src.presentation.service_bus.consumers.transcription_jobs_adapter.extract_transcription_job",
        lambda _: expected,
    )

    result = await adapter.parse_message(SimpleNamespace(message_id="M1"))

    assert result == expected


@pytest.mark.unit
@pytest.mark.asyncio
async def test_transcription_parse_message_propagates_invalid_payload(monkeypatch):
    adapter, _ = _transcription_adapter(monkeypatch)
    monkeypatch.setattr(
        "src.presentation.service_bus.consumers.transcription_jobs_adapter.extract_transcription_job",
        lambda _: (_ for _ in ()).throw(InvalidTranscriptionJobPayload("missing")),
    )

    with pytest.raises(InvalidTranscriptionJobPayload):
        await adapter.parse_message(SimpleNamespace(message_id="M1"))


@pytest.mark.unit
@pytest.mark.asyncio
async def test_transcription_process_delegates_to_use_case(monkeypatch):
    adapter, use_case = _transcription_adapter(monkeypatch)
    dto = TranscriptionJobRequestDTO(
        file_id="f1", batch_id="INIT:2026-01-28", initiative_id="INIT",
        transcription_id="ts-1", blob_url="https://x", file_name="audio.wav",
        correlation_id="cid-1",
    )

    await adapter.process(dto)

    use_case.execute.assert_awaited_once_with(dto)


# ============================================================
# EvaluationJobsConsumerAdapter
# ============================================================

@pytest.mark.unit
@pytest.mark.asyncio
async def test_evaluation_parse_message_returns_dto_on_success(monkeypatch):
    adapter, _ = _evaluation_adapter(monkeypatch)
    expected = ProcessEvaluationJobRequestDTO(
        batch_id="INIT:2026-01-28", file_id="f1", initiative_id="INIT", correlation_id="cid-1"
    )
    monkeypatch.setattr(
        "src.presentation.service_bus.consumers.evaluation_jobs_adapter.extract_evaluation_job",
        lambda _: expected,
    )

    result = await adapter.parse_message(SimpleNamespace(message_id="M1"))

    assert result == expected


@pytest.mark.unit
@pytest.mark.asyncio
async def test_evaluation_parse_message_propagates_invalid_payload(monkeypatch):
    adapter, _ = _evaluation_adapter(monkeypatch)
    monkeypatch.setattr(
        "src.presentation.service_bus.consumers.evaluation_jobs_adapter.extract_evaluation_job",
        lambda _: (_ for _ in ()).throw(InvalidEvaluationJobPayload("missing")),
    )

    with pytest.raises(InvalidEvaluationJobPayload):
        await adapter.parse_message(SimpleNamespace(message_id="M1"))


@pytest.mark.unit
@pytest.mark.asyncio
async def test_evaluation_process_delegates_to_use_case(monkeypatch):
    adapter, use_case = _evaluation_adapter(monkeypatch)
    dto = ProcessEvaluationJobRequestDTO(
        batch_id="INIT:2026-01-28", file_id="f1", initiative_id="INIT", correlation_id="cid-1"
    )

    await adapter.process(dto)

    use_case.execute.assert_awaited_once_with(dto)


# ============================================================
# BaseServiceBusConsumer lifecycle (via StorageEventsConsumerAdapter)
# ============================================================

@pytest.mark.unit
@pytest.mark.asyncio
async def test_start_creates_background_task_named_after_queue(monkeypatch):
    adapter, _ = _storage_adapter(monkeypatch)
    created = {}

    def _create_task(coro, name=None):
        created["name"] = name
        return _FakeTask(done=False)

    monkeypatch.setattr(consumer_mod.asyncio, "create_task", _create_task)

    await adapter.start()

    assert adapter._task is not None
    assert created["name"] == "consumer-q-storage"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_start_does_nothing_when_task_already_running(monkeypatch):
    adapter, _ = _storage_adapter(monkeypatch)
    create_task = Mock()
    monkeypatch.setattr(consumer_mod.asyncio, "create_task", create_task)
    adapter._task = _FakeTask(done=False)

    await adapter.start()

    create_task.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_stop_cancels_the_running_task(monkeypatch):
    adapter, _ = _storage_adapter(monkeypatch)
    task = _FakeTask(done=False, await_raises_cancelled=True)
    adapter._task = task

    await adapter.stop()

    assert task._cancelled is True


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_message_completes_on_successful_processing(monkeypatch):
    renewer = _FakeRenewer()
    monkeypatch.setattr(consumer_mod, "AutoLockRenewer", lambda: renewer)
    use_case = AsyncMock()
    adapter = StorageEventsConsumerAdapter(
        config=BaseConsumerConfig(connection_string="cs", queue_name="q"),
        process_storage_event_use_case=use_case,
    )
    dto = StorageEventRequestDTO(
        batch_id="INIT:2026-01-28", initiative_id="INIT",
        blob_url="https://x", file_name="audio.wav", container_name="c1",
        correlation_id="cid-1",
    )
    monkeypatch.setattr(
        "src.presentation.service_bus.consumers.storage_events_adapter.extract_blob_event_info",
        lambda _: dto,
    )
    receiver = _FakeReceiver()
    msg = SimpleNamespace(message_id="M1")

    await adapter._handle_message(receiver, msg)

    receiver.complete_message.assert_awaited_once_with(msg)
    receiver.abandon_message.assert_not_awaited()
    receiver.dead_letter_message.assert_not_awaited()
    assert len(renewer.register_calls) == 1


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_message_dead_letters_on_invalid_domain_exception(monkeypatch):
    adapter, _ = _storage_adapter(monkeypatch)
    monkeypatch.setattr(
        "src.presentation.service_bus.consumers.storage_events_adapter.extract_blob_event_info",
        lambda _: (_ for _ in ()).throw(InvalidStorageEvent("bad")),
    )
    receiver = _FakeReceiver()

    await adapter._handle_message(receiver, SimpleNamespace(message_id="M1"))

    receiver.dead_letter_message.assert_awaited_once()
    receiver.complete_message.assert_not_awaited()
    receiver.abandon_message.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_message_abandons_on_use_case_runtime_error(monkeypatch):
    use_case = AsyncMock()
    use_case.execute.side_effect = RuntimeError("db down")
    adapter, _ = _storage_adapter(monkeypatch, use_case=use_case)
    dto = StorageEventRequestDTO(
        batch_id="INIT:2026-01-28", initiative_id="INIT",
        blob_url="https://x", file_name="audio.wav", container_name="c1",
        correlation_id="cid-1",
    )
    monkeypatch.setattr(
        "src.presentation.service_bus.consumers.storage_events_adapter.extract_blob_event_info",
        lambda _: dto,
    )
    receiver = _FakeReceiver()
    msg = SimpleNamespace(message_id="M1")

    await adapter._handle_message(receiver, msg)

    receiver.abandon_message.assert_awaited_once_with(msg)
    receiver.complete_message.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_run_wraps_unexpected_exception_as_service_bus_consume_error(monkeypatch):
    adapter, _ = _storage_adapter(monkeypatch)

    class _ClientFactory:
        @staticmethod
        def from_connection_string(cs):
            raise RuntimeError("crash")

    monkeypatch.setattr(consumer_mod, "ServiceBusClient", _ClientFactory)

    with pytest.raises(ServiceBusConsumeError):
        await adapter._run()

    assert adapter._client is None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_run_processes_one_message_then_stops(monkeypatch):
    adapter, _ = _storage_adapter(monkeypatch)
    msg = SimpleNamespace(message_id="M1")
    receiver = _FakeReceiver(batches=[[msg], []])
    client = _FakeClient(receiver)

    class _ClientFactory:
        @staticmethod
        def from_connection_string(cs):
            return client

    monkeypatch.setattr(consumer_mod, "ServiceBusClient", _ClientFactory)
    handled = []

    async def _handle(r, m):
        handled.append(m)
        adapter._stop_event.set()

    monkeypatch.setattr(adapter, "_handle_message", _handle)
    await adapter._run()

    assert len(handled) == 1
    assert handled[0] is msg
    assert adapter._client is None
