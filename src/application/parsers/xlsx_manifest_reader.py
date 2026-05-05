from io import BytesIO
from typing import Any, Dict, Iterable, Optional
from openpyxl import load_workbook

NBSP = "\u00a0"

def _norm_text(s: str) -> str:
    s = s.replace(NBSP, " ").replace("\t", " ").strip()
    s = " ".join(s.split())
    return s

def _norm_header(h: Any) -> str:
    if h is None:
        return ""
    if isinstance(h, str):
        return _norm_text(h).casefold()
    return str(h).strip().casefold()

def _clean(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, str):
        s = _norm_text(v)
        if not s or s.casefold() in {"nan", "none", "null"}:
            return None
        return s
    return v

def norm_wav_key(v: Any) -> Optional[str]:
    v = _clean(v)
    if v is None:
        return None
    s = str(v)
    s = s.replace("\\", "/")
    s = _norm_text(s)
    if "/" in s:
        s = s.rsplit("/", 1)[-1].strip()
    return s or None

def _iter_table_rows(ws, table) -> Iterable[tuple]:
    for row in ws[table.ref]:
        yield tuple(cell.value for cell in row)

HEADER_ALIASES = {
    "wav_name": {"wav_name", "wav name", "wav", "wavname", "filename", "file_name"},
    "csv_name": {"csv_name", "csv name", "csv", "csvname"},
    "conversation_id": {"conversation_id", "conversation id", "conversationid"},
    "start_time": {"start_time", "start time"},
    "end_time": {"end_time", "end time"},
    "duration": {"duration", "duracion", "duración"},
    "xlsx_name": {"xlsx_name", "xlsx name"},
    "consecutivo": {"consecutivo", "consecutive"},
    "fecha_ocurrencia": {"fecha_ocurrencia", "fecha ocurrencia"},
    "tiempo_ocurrencia": {"tiempo_ocurrencia", "tiempo ocurrencia"},
    "activo_herope": {"activo_herope", "activo herope"},
    "tipo_reporte": {"tipo_reporte", "tipo reporte"},
    "tipo_movimiento": {"tipo_movimiento", "tipo movimiento"},
    "denominación_causa e-bitacora": {
        "denominación_causa e-bitacora",
        "denominacion_causa e-bitacora",
        "denominación causa e-bitacora",
        "denominacion causa e-bitacora",
    },
}

PUBLIC_FIELD_MAP = {
    "Wav_Name": "wav_name",
    "Csv_Name": "csv_name",
    "Conversation_ID": "conversation_id",
    "Start_Time": "start_time",
    "End_Time": "end_time",
    "Duration": "duration",
    "Xlsx_Name": "xlsx_name",
    "Consecutivo": "consecutivo",
    "Fecha_Ocurrencia": "fecha_ocurrencia",
    "Tiempo_Ocurrencia": "tiempo_ocurrencia",
    "Activo_Herope": "activo_herope",
    "Tipo_reporte": "tipo_reporte",
    "Tipo_Movimiento": "tipo_movimiento",
    "Denominación_causa E-Bitacora": "denominación_causa e-bitacora",
}


def _canonical_header(h_norm: str) -> str:
    for canon, opts in HEADER_ALIASES.items():
        if h_norm in opts:
            return canon
    return h_norm


def _select_best_table_block(ws, tables) -> list[tuple] | None:
    fallback = None
    for table in tables:
        block = list(_iter_table_rows(ws, table))
        if not block:
            continue
        headers = [_norm_header(h) for h in block[0]]
        if "wav_name" in headers:
            return block
        if fallback is None:
            fallback = block
    return fallback


def _get_candidate_blocks(ws) -> list[list[tuple]]:
    tables = list(ws.tables.values()) if getattr(ws, "tables", None) else []
    if tables:
        best = _select_best_table_block(ws, tables)
        if best:
            return [best]

    all_rows = list(ws.iter_rows(values_only=True))
    if not all_rows:
        return []
    return [all_rows]


def _build_row(headers_canon: list[str], values: tuple) -> Dict[str, Any]:
    row: Dict[str, Any] = {}
    for idx, hcanon in enumerate(headers_canon):
        if not hcanon:
            continue
        value = _clean(values[idx] if idx < len(values) else None)
        if value is not None:
            row[hcanon] = value
    return row


def _merge_manifest_row(manifest: Dict[str, Dict[str, Any]], row: Dict[str, Any]) -> None:
    key = norm_wav_key(row.get("wav_name"))
    if not key:
        return

    current = manifest.get(key, {})
    for k, v in row.items():
        if v is not None:
            current[k] = v
    manifest[key] = current


def _to_public_manifest(manifest: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    final: Dict[str, Dict[str, Any]] = {}
    for key, values in manifest.items():
        mapped = {public_key: values.get(source_key) for public_key, source_key in PUBLIC_FIELD_MAP.items()}
        final[key] = {k: v for k, v in mapped.items() if v is not None}
    return final


def parse_xlsx_manifest(xlsx: BytesIO) -> Dict[str, Dict[str, Any]]:
    wb = load_workbook(xlsx, data_only=True)
    ws = wb.active

    candidate_blocks = _get_candidate_blocks(ws)
    if not candidate_blocks:
        return {}

    manifest: Dict[str, Dict[str, Any]] = {}
    for rows in candidate_blocks:
        if not rows:
            continue
        headers_canon = [_canonical_header(_norm_header(h)) for h in rows[0]]
        for values in rows[1:]:
            if not values:
                continue
            row = _build_row(headers_canon, values)
            _merge_manifest_row(manifest, row)

    return _to_public_manifest(manifest)