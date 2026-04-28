from datetime import datetime
import pytest
from src.domain.entities.call_processing_entity import CallProcessingEntity, _parse_datetime, _parse_call_status
from src.domain.value_objects.call_processing_status import CallProcessingStatus

@pytest.mark.unit
def test_update_status_sets_status_and_updates_timestamp(monkeypatch):
    fixed = datetime(2026, 1, 28, 12, 0, 0)
    class _DT:
        @staticmethod
        def now(*args, **kwargs):
            return fixed
    import src.domain.entities.call_processing_entity as mod
    monkeypatch.setattr(mod, "datetime", _DT)
    e = CallProcessingEntity(batch_id="b1", storage_container="input", initiative_id="i1")
    e.update_status(CallProcessingStatus.PROCESSING)
    assert e.status == CallProcessingStatus.PROCESSING
    assert e.updated_at == fixed

@pytest.mark.unit
def test_mark_as_started_sets_started_at_once_and_sets_processing_if_pending(monkeypatch):
    fixed = datetime(2026, 1, 28, 12, 0, 0)
    fixed2 = datetime(2026, 1, 28, 12, 0, 1)
    class _DT:
        v = fixed
        @staticmethod
        def now(*args, **kwargs):
            return _DT.v
    import src.domain.entities.call_processing_entity as mod
    monkeypatch.setattr(mod, "datetime", _DT)
    e = CallProcessingEntity(batch_id="b1", storage_container="input", initiative_id="i1", status=CallProcessingStatus.PENDING)
    e.mark_as_started()
    assert e.started_at == fixed
    assert e.status == CallProcessingStatus.PROCESSING
    _DT.v = fixed2
    e.mark_as_started()
    assert e.started_at == fixed

@pytest.mark.unit
def test_mark_as_completed_sets_completed_at_once_and_sets_status(monkeypatch):
    fixed = datetime(2026, 1, 28, 12, 0, 0)
    fixed2 = datetime(2026, 1, 28, 12, 0, 1)
    class _DT:
        v = fixed
        @staticmethod
        def now(*args, **kwargs):
            return _DT.v
    import src.domain.entities.call_processing_entity as mod
    monkeypatch.setattr(mod, "datetime", _DT)
    e = CallProcessingEntity(batch_id="b1", storage_container="input", initiative_id="i1", status=CallProcessingStatus.PROCESSING)
    e.mark_as_completed()
    assert e.status == CallProcessingStatus.COMPLETED
    assert e.completed_at == fixed
    _DT.v = fixed2
    e.mark_as_completed()
    assert e.completed_at == fixed

@pytest.mark.unit
def test_increment_completed_increments_completed_and_processed(monkeypatch):
    fixed = datetime(2026, 1, 28, 12, 0, 0)
    class _DT:
        @staticmethod
        def now(*args, **kwargs):
            return fixed
    import src.domain.entities.call_processing_entity as mod
    monkeypatch.setattr(mod, "datetime", _DT)
    e = CallProcessingEntity(batch_id="b1", storage_container="input", initiative_id="i1", total_files=10)
    e.increment_completed()
    assert e.completed_files == 1
    assert e.processed_files == 1
    assert e.updated_at == fixed

@pytest.mark.unit
def test_increment_failed_increments_failed_and_processed(monkeypatch):
    fixed = datetime(2026, 1, 28, 12, 0, 0)
    class _DT:
        @staticmethod
        def now(*args, **kwargs):
            return fixed
    import src.domain.entities.call_processing_entity as mod
    monkeypatch.setattr(mod, "datetime", _DT)
    e = CallProcessingEntity(batch_id="b1", storage_container="input", initiative_id="i1", total_files=10)
    e.increment_failed()
    assert e.failed_files == 1
    assert e.processed_files == 1
    assert e.updated_at == fixed

@pytest.mark.unit
def test_increment_total_files_increments_total():
    e = CallProcessingEntity(batch_id="b1", storage_container="input", initiative_id="i1")
    e.increment_total_files()
    assert e.total_files == 1

@pytest.mark.unit
def test_check_completion_marks_failed_if_all_failed(monkeypatch):
    fixed = datetime(2026, 1, 28, 12, 0, 0)
    class _DT:
        @staticmethod
        def now(*args, **kwargs):
            return fixed
    import src.domain.entities.call_processing_entity as mod
    monkeypatch.setattr(mod, "datetime", _DT)
    e = CallProcessingEntity(batch_id="b1", storage_container="input", initiative_id="i1", total_files=2)
    e.increment_failed()
    e.increment_failed()
    e.check_completion()
    assert e.status == CallProcessingStatus.FAILED
    assert e.completed_at is None

@pytest.mark.unit
def test_check_completion_marks_completed_if_not_all_failed(monkeypatch):
    fixed = datetime(2026, 1, 28, 12, 0, 0)
    class _DT:
        @staticmethod
        def now(*args, **kwargs):
            return fixed
    import src.domain.entities.call_processing_entity as mod
    monkeypatch.setattr(mod, "datetime", _DT)
    e = CallProcessingEntity(batch_id="b1", storage_container="input", initiative_id="i1", total_files=2)
    e.increment_completed()
    e.increment_failed()
    e.check_completion()
    assert e.status == CallProcessingStatus.COMPLETED
    assert e.completed_at == fixed

@pytest.mark.unit
@pytest.mark.parametrize(
    "value,expected",
    [
        (None, None),
        ("2026-01-28T12:00:00+00:00", datetime.fromisoformat("2026-01-28T12:00:00+00:00")),
        (datetime(2026, 1, 28, 12, 0, 0), datetime(2026, 1, 28, 12, 0, 0)),
        (123, None),
    ],
)
def test_parse_datetime(value, expected):
    out = _parse_datetime(value)
    assert out == expected

@pytest.mark.unit
def test_from_dict_maps_fields():
    now = datetime(2026, 1, 28, 12, 0, 0)
    data = {
        "batch_id": "b1",
        "initiative_id": "i1",
        "storage_container": "c",
        "total_files": 10,
        "processed_files": 3,
        "completed_files": 2,
        "failed_files": 1,
        "status": CallProcessingStatus.PROCESSING,
        "created_at": now,
        "started_at": now,
        "completed_at": None,
        "updated_at": now,
    }
    e = CallProcessingEntity.from_dict(data)
    assert e.batch_id == "b1"
    assert e.initiative_id == "i1"
    assert e.storage_container == "c"
    assert e.total_files == 10
    assert e.processed_files == 3
    assert e.completed_files == 2
    assert e.failed_files == 1
    assert e.status == CallProcessingStatus.PROCESSING
    assert e.created_at == now
    assert e.started_at == now
    assert e.completed_at is None
    assert e.updated_at == now