import json
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from azure.servicebus.exceptions import ServiceBusConnectionError, OperationTimeoutError
from src.infrastructure.adapters.service_bus.publisher import BaseServiceBusPublisher, BasePublisherConfig
from src.infrastructure.adapters.service_bus.publishers.transcription_jobs_adapter import TranscriptionJobsPublisherAdapter
from src.infrastructure.adapters.service_bus.publishers.evaluation_jobs_adapter import EvaluationJobsPublisherAdapter
from src.infrastructure.adapters.service_bus.publishers.document_generation_jobs_adapter import DocumentGenerationJobsPublisherAdapter
from src.infrastructure.adapters.service_bus.factories.transcription_job_message_factory import build_transcription_job_message
from src.infrastructure.adapters.service_bus.factories.evaluation_job_message_factory import build_evaluation_job_message
from src.infrastructure.adapters.service_bus.factories.document_generation_job_message_factory import build_document_generation_job_message
from src.infrastructure.exceptions.service_bus_exceptions import ServiceBusPublishError
from src.domain.entities.transcription_job_entity import TranscriptionJobEntity
from src.domain.entities.evaluation_job_entity import EvaluationJobEntity
from src.domain.entities.document_generation_job_entity import DocumentGenerationJobEntity


def _transcription_job():
    return TranscriptionJobEntity(
        batch_id="INIT:2026-01-28",
        blob_url="https://storage/audio.wav",
        file_name="audio.wav",
        file_id="f1",
        initiative_id="INIT",
        storage_container="c1",
        transcription_id="ts-1",
        correlation_id="INIT:2026-01-28",
    )


def _evaluation_job():
    return EvaluationJobEntity(batch_id="INIT:2026-01-28", file_id="f1", initiative_id="INIT", correlation_id="cid-1")


def _doc_gen_job():
    return DocumentGenerationJobEntity(batch_id="INIT:2026-01-28", initiative_id="INIT", correlation_id="cid-1")


def _config(queue_name="test-queue"):
    return BasePublisherConfig(connection_string="cs", queue_name=queue_name)


def _read_message_body(msg) -> dict:
    raw = b"".join(bytes(x) for x in msg.body)
    return json.loads(raw.decode("utf-8"))


def _make_sb_mock():
    sender = AsyncMock()
    inner_cm = MagicMock()
    inner_cm.__aenter__ = AsyncMock(return_value=sender)
    inner_cm.__aexit__ = AsyncMock(return_value=False)
    client = MagicMock()
    client.get_queue_sender = MagicMock(return_value=inner_cm)
    outer_cm = MagicMock()
    outer_cm.__aenter__ = AsyncMock(return_value=client)
    outer_cm.__aexit__ = AsyncMock(return_value=False)
    return outer_cm, sender


class _ConcretePublisher(BaseServiceBusPublisher):
    pass


# ============================================================
# Message Factories
# ============================================================

@pytest.mark.unit
def test_build_transcription_job_message_encodes_all_entity_fields():
    job = _transcription_job()

    msg = build_transcription_job_message(job)
    body = _read_message_body(msg)

    assert body["batch_id"] == "INIT:2026-01-28"
    assert body["file_id"] == "f1"
    assert body["transcription_id"] == "ts-1"
    assert body["storage_container"] == "c1"


@pytest.mark.unit
def test_build_transcription_job_message_sets_message_id_to_file_id():
    msg = build_transcription_job_message(_transcription_job())

    assert msg.message_id == "f1"


@pytest.mark.unit
def test_build_transcription_job_message_sets_correlation_id_to_batch_id():
    msg = build_transcription_job_message(_transcription_job())

    assert msg.correlation_id == "INIT:2026-01-28"


@pytest.mark.unit
def test_build_evaluation_job_message_encodes_all_entity_fields():
    msg = build_evaluation_job_message(_evaluation_job())
    body = _read_message_body(msg)

    assert body["batch_id"] == "INIT:2026-01-28"
    assert body["file_id"] == "f1"
    assert body["initiative_id"] == "INIT"


@pytest.mark.unit
def test_build_evaluation_job_message_sets_message_id_to_file_id():
    msg = build_evaluation_job_message(_evaluation_job())

    assert msg.message_id == "f1"


@pytest.mark.unit
def test_build_document_generation_job_message_encodes_all_entity_fields():
    msg = build_document_generation_job_message(_doc_gen_job())
    body = _read_message_body(msg)

    assert body["batch_id"] == "INIT:2026-01-28"
    assert body["initiative_id"] == "INIT"


@pytest.mark.unit
def test_build_document_generation_job_message_sets_message_id_to_batch_id():
    msg = build_document_generation_job_message(_doc_gen_job())

    assert msg.message_id == "INIT:2026-01-28"


# ============================================================
# BaseServiceBusPublisher
# ============================================================

@pytest.mark.unit
@pytest.mark.asyncio
async def test_publish_sends_message_immediately_when_no_schedule(monkeypatch):
    outer_cm, sender = _make_sb_mock()
    monkeypatch.setattr(
        "src.infrastructure.adapters.service_bus.publisher.ServiceBusClient",
        MagicMock(from_connection_string=MagicMock(return_value=outer_cm)),
    )
    publisher = _ConcretePublisher(_config())
    msg = MagicMock()

    await publisher.publish(msg)

    sender.send_messages.assert_awaited_once_with(msg)
    sender.schedule_messages.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_publish_schedules_message_when_enqueue_time_provided(monkeypatch):
    outer_cm, sender = _make_sb_mock()
    monkeypatch.setattr(
        "src.infrastructure.adapters.service_bus.publisher.ServiceBusClient",
        MagicMock(from_connection_string=MagicMock(return_value=outer_cm)),
    )
    publisher = _ConcretePublisher(_config())
    msg = MagicMock()
    scheduled_time = datetime(2026, 1, 28, 12, 0, tzinfo=timezone.utc)

    await publisher.publish(msg, scheduled_enqueue_time_utc=scheduled_time)

    sender.schedule_messages.assert_awaited_once_with(msg, scheduled_time)
    sender.send_messages.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_publish_wraps_connection_error_as_service_bus_publish_error(monkeypatch):
    monkeypatch.setattr(
        "src.infrastructure.adapters.service_bus.publisher.ServiceBusClient",
        MagicMock(from_connection_string=MagicMock(side_effect=ServiceBusConnectionError())),
    )
    publisher = _ConcretePublisher(_config())

    with pytest.raises(ServiceBusPublishError):
        await publisher.publish(MagicMock())


@pytest.mark.unit
@pytest.mark.asyncio
async def test_publish_wraps_timeout_as_service_bus_publish_error(monkeypatch):
    monkeypatch.setattr(
        "src.infrastructure.adapters.service_bus.publisher.ServiceBusClient",
        MagicMock(from_connection_string=MagicMock(side_effect=OperationTimeoutError())),
    )
    publisher = _ConcretePublisher(_config())

    with pytest.raises(ServiceBusPublishError):
        await publisher.publish(MagicMock())


@pytest.mark.unit
@pytest.mark.asyncio
async def test_publish_wraps_generic_exception_as_service_bus_publish_error(monkeypatch):
    monkeypatch.setattr(
        "src.infrastructure.adapters.service_bus.publisher.ServiceBusClient",
        MagicMock(from_connection_string=MagicMock(side_effect=RuntimeError("unexpected"))),
    )
    publisher = _ConcretePublisher(_config())

    with pytest.raises(ServiceBusPublishError):
        await publisher.publish(MagicMock())


# ============================================================
# TranscriptionJobsPublisherAdapter
# ============================================================

@pytest.mark.unit
@pytest.mark.asyncio
async def test_transcription_publisher_enqueue_sends_message_with_correct_body(monkeypatch):
    outer_cm, sender = _make_sb_mock()
    monkeypatch.setattr(
        "src.infrastructure.adapters.service_bus.publisher.ServiceBusClient",
        MagicMock(from_connection_string=MagicMock(return_value=outer_cm)),
    )
    adapter = TranscriptionJobsPublisherAdapter(config=_config("q-transcription"))

    await adapter.enqueue(_transcription_job())

    sender.send_messages.assert_awaited_once()
    body = _read_message_body(sender.send_messages.await_args.args[0])
    assert body["file_id"] == "f1"
    assert body["transcription_id"] == "ts-1"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_transcription_publisher_enqueue_with_scheduled_time_schedules_message(monkeypatch):
    outer_cm, sender = _make_sb_mock()
    monkeypatch.setattr(
        "src.infrastructure.adapters.service_bus.publisher.ServiceBusClient",
        MagicMock(from_connection_string=MagicMock(return_value=outer_cm)),
    )
    adapter = TranscriptionJobsPublisherAdapter(config=_config())
    scheduled_time = datetime(2026, 1, 28, 12, 0, tzinfo=timezone.utc)

    await adapter.enqueue(_transcription_job(), scheduled_enqueue_time_utc=scheduled_time)

    sender.schedule_messages.assert_awaited_once()
    assert sender.schedule_messages.await_args.args[1] == scheduled_time


# ============================================================
# EvaluationJobsPublisherAdapter
# ============================================================

@pytest.mark.unit
@pytest.mark.asyncio
async def test_evaluation_publisher_enqueue_sends_message_with_correct_body(monkeypatch):
    outer_cm, sender = _make_sb_mock()
    monkeypatch.setattr(
        "src.infrastructure.adapters.service_bus.publisher.ServiceBusClient",
        MagicMock(from_connection_string=MagicMock(return_value=outer_cm)),
    )
    adapter = EvaluationJobsPublisherAdapter(config=_config("q-evaluation"))

    await adapter.enqueue(_evaluation_job())

    sender.send_messages.assert_awaited_once()
    body = _read_message_body(sender.send_messages.await_args.args[0])
    assert body["file_id"] == "f1"
    assert body["initiative_id"] == "INIT"


# ============================================================
# DocumentGenerationJobsPublisherAdapter
# ============================================================

@pytest.mark.unit
@pytest.mark.asyncio
async def test_document_generation_publisher_enqueue_sends_message_with_correct_body(monkeypatch):
    outer_cm, sender = _make_sb_mock()
    monkeypatch.setattr(
        "src.infrastructure.adapters.service_bus.publisher.ServiceBusClient",
        MagicMock(from_connection_string=MagicMock(return_value=outer_cm)),
    )
    adapter = DocumentGenerationJobsPublisherAdapter(config=_config("q-doc-gen"))

    await adapter.enqueue(_doc_gen_job())

    sender.send_messages.assert_awaited_once()
    body = _read_message_body(sender.send_messages.await_args.args[0])
    assert body["batch_id"] == "INIT:2026-01-28"
    assert body["initiative_id"] == "INIT"
