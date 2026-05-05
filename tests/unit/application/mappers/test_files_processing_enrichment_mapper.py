import pytest
from datetime import datetime, time
from src.application.mappers.files_processing_enrichment_mapper import (
    _parse_excel_datetime,
    _parse_excel_duration_mmss,
    _parse_excel_time_str,
    _prune_none,
    to_files_evaluation_enrichment_entity,
    to_files_xlsx_enrichment_entity,
)
from src.domain.entities.files_processing_entity import EvaluationMetadata


@pytest.mark.unit
def test_parse_excel_time_str_parses_12h_string_with_spanish_suffix():
    value = "10:05:00 p. m."

    result = _parse_excel_time_str(value)

    assert result == "22:05:00"


@pytest.mark.unit
def test_parse_excel_time_str_parses_24h_string():
    value = "08:15:30"

    result = _parse_excel_time_str(value)

    assert result == "08:15:30"


@pytest.mark.unit
def test_parse_excel_time_str_returns_none_for_invalid_string():
    value = "invalid-time"

    result = _parse_excel_time_str(value)

    assert result is None


@pytest.mark.unit
def test_parse_excel_time_str_returns_from_datetime_input():
    value = datetime(2026, 1, 28, 8, 15, 30)

    result = _parse_excel_time_str(value)

    assert result == "08:15:30"


@pytest.mark.unit
def test_parse_excel_time_str_returns_from_time_input():
    value = time(8, 15, 30)

    result = _parse_excel_time_str(value)

    assert result == "08:15:30"


@pytest.mark.unit
def test_parse_excel_time_str_returns_none_for_non_string_non_time_input():
    value = 123

    result = _parse_excel_time_str(value)

    assert result is None


@pytest.mark.unit
def test_parse_excel_time_str_returns_none_for_blank_string():
    value = "   "

    result = _parse_excel_time_str(value)

    assert result is None


@pytest.mark.unit
def test_parse_excel_datetime_handles_datetime_blank_string_and_none():
    now = datetime(2026, 1, 28, 10, 0, 0)

    result_from_datetime = _parse_excel_datetime(now)
    result_from_blank = _parse_excel_datetime("   ")
    result_from_none = _parse_excel_datetime(None)

    assert result_from_datetime == now
    assert result_from_blank is None
    assert result_from_none is None


@pytest.mark.unit
def test_parse_excel_duration_mmss_handles_time_and_string_variants():
    result_time = _parse_excel_duration_mmss(time(0, 2, 10))
    result_hms = _parse_excel_duration_mmss("01:02:03")
    result_ms = _parse_excel_duration_mmss("02:10")

    assert result_time == "02:10"
    assert result_hms == "62:03"
    assert result_ms == "02:10"


@pytest.mark.unit
def test_parse_excel_duration_mmss_returns_none_for_invalid_values():
    result_blank = _parse_excel_duration_mmss("   ")
    result_invalid_parts = _parse_excel_duration_mmss("bad")
    result_invalid_numbers = _parse_excel_duration_mmss("aa:bb")
    result_other_type = _parse_excel_duration_mmss(123)
    result_none = _parse_excel_duration_mmss(None)

    assert result_blank is None
    assert result_invalid_parts is None
    assert result_invalid_numbers is None
    assert result_other_type is None
    assert result_none is None


@pytest.mark.unit
def test_to_files_xlsx_enrichment_entity_uses_start_time_when_fecha_and_tiempo_are_missing():
    row = {
        "Start_Time": "2026 Jan 28 10:15:30 PM",
        "End_Time": "23:59:59",
        "Duration": "00:02:10",
        "Denominacion_causa E-Bitacora": "cause",
    }

    entity = to_files_xlsx_enrichment_entity(row)

    assert entity.operative_event is not None
    assert entity.operative_event.time_occurrence == "22:15:30"
    assert entity.operative_event.designation_cause_e_logbook == "cause"
    assert entity.evaluation_result.metadata.date_recording == "2026-01-28"
    assert entity.evaluation_result.metadata.start_time == "22:15:30"
    assert entity.evaluation_result.metadata.end_time == "23:59:59"
    assert entity.evaluation_result.metadata.duration_format == "02:10"


@pytest.mark.unit
def test_to_files_evaluation_enrichment_entity_preserves_existing_metadata_and_defaults_scores():
    evaluation_json = {
        "evaluation_result": {
            "metadata": {"cct_engineer": "CCT1", "se_operator": "SE1", "xm_engineer": "XM1"},
            "evaluation": {
                "questions": [{"id": 1}],
                "total_points": {"cct_engineer": 10},
                "average": {"cct_engineer": 1.0},
            },
        },
    }
    existing_metadata = EvaluationMetadata(
        date_recording="2026-01-28",
        start_time="10:00:00",
        end_time="10:10:00",
        duration_format="10:00",
    )

    entity = to_files_evaluation_enrichment_entity(
        evaluation_json,
        existing_metadata=existing_metadata,
    )

    assert entity.engineer_score == 0.0
    assert entity.operator_score == 0.0
    assert entity.evaluation_result.metadata.date_recording == "2026-01-28"
    assert entity.evaluation_result.metadata.start_time == "10:00:00"
    assert entity.evaluation_result.metadata.end_time == "10:10:00"
    assert entity.evaluation_result.metadata.duration_format == "10:00"
    assert entity.evaluation_result.metadata.cct_engineer == "CCT1"
    assert entity.evaluation_result.metadata.xm_engineer == "XM1"


@pytest.mark.unit
def test_to_files_evaluation_enrichment_entity_casts_scores_to_float():
    evaluation_json = {
        "engineer_score": "4.5",
        "operator_score": "3.0",
        "evaluation_result": {"evaluation": {}},
    }

    entity = to_files_evaluation_enrichment_entity(
        evaluation_json,
        existing_metadata=None,
    )

    assert entity.engineer_score == 4.5
    assert entity.operator_score == 3.0


@pytest.mark.unit
def test_prune_none_handles_nested_dicts_and_lists():
    value = {"a": None, "b": [{"c": 1}, {"d": None}]}

    result = _prune_none(value)

    assert result == {"b": [{"c": 1}, {}]}
