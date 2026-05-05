from dataclasses import asdict
from datetime import datetime, time
from typing import Any, Mapping
from src.domain.entities.files_evaluation_enrichment_entity import (
    FilesEvaluationEnrichmentEntity,
)
from src.domain.entities.files_processing_entity import (
    Evaluation,
    EvaluationMetadata,
    EvaluationResult,
    OperativeEvent,
)
from src.domain.entities.files_xlsx_enrichment_entity import FilesXlsxEnrichmentEntity

def to_files_xlsx_enrichment_entity(
    row: Mapping[str, Any],
) -> FilesXlsxEnrichmentEntity:
    operative_event = OperativeEvent(
        date_occurrence=(
            _parse_excel_datetime(row.get("Fecha_Ocurrencia"))
            if row.get("Fecha_Ocurrencia") is not None
            else _parse_excel_datetime(row.get("Start_Time"))
        ),
        time_occurrence=(
            _parse_excel_time_str(row.get("Tiempo_Ocurrencia"))
            if row.get("Tiempo_Ocurrencia") is not None
            else _parse_excel_time_str(row.get("Start_Time"))
        ),
        herope_active=row.get("Activo_Herope"),
        report_type=row.get("Tipo_reporte"),
        movement_type=row.get("Tipo_Movimiento"),
        designation_cause_e_logbook=(
            row.get("Denominacion_causa E-Bitacora")
            or row.get("Denominaci\u00f3n_causa E-Bitacora")
        ),
    )
    if _prune_none(asdict(operative_event)) == {}:
        operative_event = None
    return FilesXlsxEnrichmentEntity(
        xlsx_name=row.get("Xlsx_Name"),
        csv_name=row.get("Csv_Name"),
        conversation_id=row.get("Conversation_ID"),
        consecutive=row.get("Consecutivo"),
        operative_event=operative_event,
        evaluation_result=EvaluationResult(
            metadata=EvaluationMetadata(
                date_recording=(
                    _parse_excel_date_str(row.get("Fecha_Ocurrencia"))
                    if row.get("Fecha_Ocurrencia") is not None
                    else _parse_excel_date_str(row.get("Start_Time"))
                ),
                start_time=_parse_excel_time_str(row.get("Start_Time")),
                end_time=_parse_excel_time_str(row.get("End_Time")),
                duration_format=_parse_excel_duration_mmss(row.get("Duration")),
            )
        ),
    )

def to_files_evaluation_enrichment_entity(
    evaluation_json: Mapping[str, Any],
    *,
    existing_metadata: EvaluationMetadata | None,
) -> FilesEvaluationEnrichmentEntity:
    evaluation_result = evaluation_json.get("evaluation_result") or {}
    metadata = evaluation_result.get("metadata") or {}
    score = evaluation_result.get("evaluation") or {}
    return FilesEvaluationEnrichmentEntity(
        cct_engineer=evaluation_json.get("cct_engineer"),
        se_operator=evaluation_json.get("se_operator"),
        substation=evaluation_json.get("substation"),
        engineer_score=(
            float(evaluation_json.get("engineer_score"))
            if evaluation_json.get("engineer_score") is not None
            else 0.0
        ),
        operator_score=(
            float(evaluation_json.get("operator_score"))
            if evaluation_json.get("operator_score") is not None
            else 0.0
        ),
        evaluation_result=EvaluationResult(
            metadata=EvaluationMetadata(
                date_recording=getattr(existing_metadata, "date_recording", None),
                start_time=getattr(existing_metadata, "start_time", None),
                end_time=getattr(existing_metadata, "end_time", None),
                duration_format=getattr(existing_metadata, "duration_format", None),
                cct_engineer=metadata.get("cct_engineer"),
                se_operator=metadata.get("se_operator"),
                xm_engineer=metadata.get("xm_engineer"),
            ),
            evaluation=Evaluation(
                questions=score.get("questions", []),
                total_points=score.get("total_points", 0),
                average=score.get("average", 0),
            ),
            observations=evaluation_result.get("observations"),
        ),
    )

def _parse_excel_datetime(value: Any):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        return datetime.strptime(stripped, "%Y %b %d %I:%M:%S %p")
    return None

def _parse_excel_date_str(value: Any) -> str | None:
    parsed = _parse_excel_datetime(value)
    return parsed.date().isoformat() if parsed else None

def _parse_excel_time_str(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime("%H:%M:%S")
    if isinstance(value, time):
        return f"{value.hour:02d}:{value.minute:02d}:{value.second:02d}"
    if not isinstance(value, str):
        return None

    stripped = value.strip()
    if not stripped:
        return None

    normalized = _normalize_time_text(stripped)
    parsed_12h = _parse_12h_time(normalized)
    if parsed_12h:
        return parsed_12h

    parsed_24h = _parse_24h_time(normalized)
    if parsed_24h:
        return parsed_24h

    return _parse_time_from_datetime_string(stripped)


def _normalize_time_text(value: str) -> str:
    return (
        value.lower()
        .replace("\u00a0", " ")
        .replace("a. m.", "am")
        .replace("p. m.", "pm")
        .replace("a.m.", "am")
        .replace("p.m.", "pm")
        .replace(".", "")
        .strip()
    )


def _parse_12h_time(value: str) -> str | None:
    if not value.endswith(("am", "pm")):
        return None
    compacted = value.replace(" ", "")
    try:
        return datetime.strptime(compacted, "%I:%M:%S%p").strftime("%H:%M:%S")
    except ValueError:
        return None


def _parse_24h_time(value: str) -> str | None:
    try:
        return datetime.strptime(value, "%H:%M:%S").strftime("%H:%M:%S")
    except ValueError:
        return None


def _parse_time_from_datetime_string(value: str) -> str | None:
    try:
        parsed = _parse_excel_datetime(value)
    except ValueError:
        return None
    return parsed.strftime("%H:%M:%S") if parsed else None

def _parse_excel_duration_mmss(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, time):
        total_seconds = value.hour * 3600 + value.minute * 60 + value.second
    elif isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        parts = stripped.split(":")
        try:
            if len(parts) == 3:
                hours, minutes, seconds = map(int, parts)
                total_seconds = hours * 3600 + minutes * 60 + seconds
            elif len(parts) == 2:
                minutes, seconds = map(int, parts)
                total_seconds = minutes * 60 + seconds
            else:
                return None
        except ValueError:
            return None
    else:
        return None
    minutes = total_seconds // 60
    seconds = total_seconds % 60
    return f"{minutes:02d}:{seconds:02d}"

def _prune_none(value: Any):
    if isinstance(value, dict):
        return {k: _prune_none(v) for k, v in value.items() if v is not None}
    if isinstance(value, list):
        return [_prune_none(item) for item in value]
    return value
