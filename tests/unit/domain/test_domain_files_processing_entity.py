from datetime import datetime, timezone
import pytest
from src.domain.entities.files_processing_entity import (
    Evaluation,
    EvaluationMetadata,
    EvaluationResult,
    FilesProcessingEntity,
    OperativeEvent,
    _parse_datetime,
)
from src.domain.value_objects.files_processing_status import FilesProcessingStatus

@pytest.mark.unit
def test_update_status_sets_processing_started_at_once(monkeypatch):
    fixed = datetime(2026, 1, 28, 12, 0, 0)
    fixed2 = datetime(2026, 1, 28, 12, 0, 1)
    class _DT:
        v = fixed
        @staticmethod
        def now(*args, **kwargs):
            return _DT.v
    import src.domain.entities.files_processing_entity as mod
    monkeypatch.setattr(mod, "datetime", _DT)
    e = FilesProcessingEntity(blob_url="https://xyz", batch_id="b1", file_name="a.wav", file_id="f1")
    assert e.processing_started_at is None
    e.update_status(FilesProcessingStatus.PROCESSING)
    assert e.status == FilesProcessingStatus.PROCESSING
    assert e.processing_started_at == fixed
    _DT.v = fixed2
    e.update_status(FilesProcessingStatus.PROCESSING)
    assert e.processing_started_at == fixed

@pytest.mark.unit
def test_update_status_sets_completed_at_once_for_completed(monkeypatch):
    fixed = datetime(2026, 1, 28, 12, 0, 0)
    fixed2 = datetime(2026, 1, 28, 12, 0, 1)
    class _DT:
        v = fixed
        @staticmethod
        def now(*args, **kwargs):
            return _DT.v
    import src.domain.entities.files_processing_entity as mod
    monkeypatch.setattr(mod, "datetime", _DT)
    e = FilesProcessingEntity(blob_url="https://xyz", batch_id="b1", file_name="a.wav", file_id="f1")
    e.mark_as_completed()
    assert e.status == FilesProcessingStatus.COMPLETED
    assert e.completed_at == fixed
    _DT.v = fixed2
    e.mark_as_completed()
    assert e.completed_at == fixed

@pytest.mark.unit
def test_update_status_sets_completed_at_once_for_failed(monkeypatch):
    fixed = datetime(2026, 1, 28, 12, 0, 0)
    class _DT:
        @staticmethod
        def now(*args, **kwargs):
            return fixed
    import src.domain.entities.files_processing_entity as mod
    monkeypatch.setattr(mod, "datetime", _DT)
    e = FilesProcessingEntity(blob_url="https://xyz", batch_id="b1", file_name="a.wav", file_id="f1")
    e.mark_as_failed()
    assert e.status == FilesProcessingStatus.FAILED
    assert e.completed_at == fixed

@pytest.mark.unit
def test_setters_assign_values():
    now = datetime(2026, 1, 28, 12, 0, 0, tzinfo=timezone.utc)
    e = FilesProcessingEntity(blob_url="https://xyz", batch_id="b1", file_name="a.wav", file_id="f1", transcription="t0")
    e.set_transcription("t1")
    assert e.transcription == "t1"
    op = OperativeEvent(
        date_occurrence=now,
        time_occurrence="12:00",
        herope_active="x",
        report_type="r",
        movement_type="m",
        designation_cause_e_logbook="d",
    )
    e.set_operative_event(op)
    assert e.operative_event == op
    md = EvaluationMetadata(
        date_recording="2026-01-28",
        start_time="12:00",
        end_time="12:01",
        duration_format="00:01:00",
        cct_engineer="e",
        se_operator="o",
        xm_engineer=None,
    )
    ev = Evaluation(questions=[], total_points={}, average={})
    er = EvaluationResult(metadata=md, evaluation=ev, observations=None)
    e.set_evaluation_result(er)
    assert e.evaluation_result == er

@pytest.mark.unit
def test_parse_datetime_handles_none_datetime_iso_and_other():
    iso = "2026-01-28T12:00:00+00:00"
    dt = datetime.fromisoformat(iso)
    assert _parse_datetime(None) is None
    assert _parse_datetime(dt) == dt
    assert _parse_datetime(iso) == dt
    assert _parse_datetime(123) is None

@pytest.mark.unit
def test_metadata_from_dict_returns_none_when_not_dict():
    assert EvaluationMetadata.from_dict(None) is None
    assert EvaluationMetadata.from_dict("x") is None

@pytest.mark.unit
def test_metadata_from_dict_maps_optionals_and_blanks_to_none():
    md = EvaluationMetadata.from_dict(
        {
            "date_recording": "",
            "start_time": "10:00:00",
            "end_time": None,
            "duration_format": "00:01:00",
            "cct_engineer": "e",
            "se_operator": "o",
            "xm_engineer": "",
        }
    )
    assert md.date_recording is None
    assert md.start_time == "10:00:00"
    assert md.end_time is None
    assert md.duration_format == "00:01:00"
    assert md.cct_engineer == "e"
    assert md.se_operator == "o"
    assert md.xm_engineer is None

@pytest.mark.unit
def test_evaluation_from_dict_returns_none_when_not_dict():
    assert Evaluation.from_dict(None) is None
    assert Evaluation.from_dict("x") is None

@pytest.mark.unit
def test_evaluation_from_dict_defaults_collections():
    ev = Evaluation.from_dict({"questions": None, "total_points": None, "average": None})
    assert ev.questions == []
    assert ev.total_points == {}
    assert ev.average == {}

@pytest.mark.unit
def test_evaluation_result_from_dict_returns_none_when_not_dict():
    assert EvaluationResult.from_dict(None) is None
    assert EvaluationResult.from_dict("x") is None

@pytest.mark.unit
def test_evaluation_result_from_dict_maps_nested_objects():
    er = EvaluationResult.from_dict(
        {
            "metadata": {"date_recording": "2026-01-01"},
            "evaluation": {"questions": [{"q": 1}], "total_points": {"a": 1}, "average": {"a": 1}},
            "observations": "",
        }
    )
    assert er.metadata is not None
    assert er.metadata.date_recording == "2026-01-01"
    assert er.evaluation is not None
    assert er.evaluation.questions == [{"q": 1}]
    assert er.observations is None

@pytest.mark.unit
def test_operative_event_from_dict_returns_none_when_not_dict():
    assert OperativeEvent.from_dict(None) is None
    assert OperativeEvent.from_dict("x") is None

@pytest.mark.unit
def test_operative_event_from_dict_maps_fields_and_parses_datetime():
    now = datetime(2026, 1, 28, 12, 0, 0, tzinfo=timezone.utc)
    op = OperativeEvent.from_dict(
        {
            "date_occurrence": now.isoformat(),
            "time_occurrence": "12:00",
            "herope_active": "",
            "report_type": "r",
            "movement_type": None,
            "designation_cause_e_logbook": "d",
        }
    )
    assert op.date_occurrence == datetime.fromisoformat(now.isoformat())
    assert op.time_occurrence == "12:00"
    assert op.herope_active is None
    assert op.report_type == "r"
    assert op.movement_type is None
    assert op.designation_cause_e_logbook == "d"

@pytest.mark.unit
def test_from_dict_maps_nested_objects_and_optionals():
    now = datetime(2026, 1, 28, 12, 0, 0, tzinfo=timezone.utc)
    data = {
        "file_id": "f1",
        "batch_id": "b1",
        "file_name": "a.wav",
        "blob_url": "https://xyz",
        "conversation_id": "cv1",
        "consecutive": "1",
        "csv_name": "a.csv",
        "xlsx_name": "a.xlsx",
        "transcription": "t",
        "cct_engineer": "e",
        "se_operator": "o",
        "substation": "s",
        "engineer_score": 1.0,
        "operator_score": 2.0,
        "operative_event": {
            "date_occurrence": now,
            "time_occurrence": "12:00",
            "herope_active": "x",
            "report_type": "r",
            "movement_type": "m",
            "designation_cause_e_logbook": "d",
        },
        "evaluation_result": {
            "metadata": {
                "date_recording": "2026-01-28",
                "start_time": "12:00",
                "end_time": "12:01",
                "duration_format": "00:01:00",
                "cct_engineer": "e",
                "se_operator": "o",
                "xm_engineer": "",
            },
            "evaluation": {"questions": None, "total_points": None, "average": None},
            "observations": "",
        },
        "status": FilesProcessingStatus.PENDING,
        "error_message": "",
        "created_at": now,
        "processing_started_at": None,
        "completed_at": None,
        "updated_at": now,
    }
    e = FilesProcessingEntity.from_dict(data)
    assert e.file_id == "f1"
    assert e.batch_id == "b1"
    assert e.file_name == "a.wav"
    assert e.blob_url == "https://xyz"
    assert e.error_message is None
    assert e.operative_event is not None
    assert e.operative_event.time_occurrence == "12:00"
    assert e.evaluation_result is not None
    assert e.evaluation_result.metadata is not None
    assert e.evaluation_result.metadata.xm_engineer is None
    assert e.evaluation_result.evaluation is not None
    assert e.evaluation_result.evaluation.questions == []
    assert e.status == FilesProcessingStatus.PENDING
    assert e.created_at == now
    assert e.updated_at == now