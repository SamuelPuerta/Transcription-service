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

def parse_xlsx_manifest(xlsx: BytesIO) -> Dict[str, Dict[str, Any]]:
    wb = load_workbook(xlsx, data_only=True)
    ws = wb.active
    tables = list(ws.tables.values()) if getattr(ws, "tables", None) else []
    candidate_blocks: list[list[tuple]] = []
    if tables:
        best = None
        for t in tables:
            block = list(_iter_table_rows(ws, t))
            if not block:
                continue
            headers = [_norm_header(h) for h in block[0]]
            if "wav_name" in headers:
                best = block
                break
            if best is None:
                best = block
        if best:
            candidate_blocks.append(best)
    if not candidate_blocks:
        all_rows = list(ws.iter_rows(values_only=True))
        if not all_rows:
            return {}
        candidate_blocks.append(all_rows)
    manifest: Dict[str, Dict[str, Any]] = {}
    header_aliases = {
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
    def canonical_header(h_norm: str) -> str:
        for canon, opts in header_aliases.items():
            if h_norm in opts:
                return canon
        return h_norm
    for rows in candidate_blocks:
        if not rows:
            continue
        raw_headers = list(rows[0])
        headers_norm = [_norm_header(h) for h in raw_headers]
        headers_canon = [canonical_header(h) for h in headers_norm]
        for r in rows[1:]:
            if not r:
                continue
            row: Dict[str, Any] = {}
            for i, hcanon in enumerate(headers_canon):
                if not hcanon:
                    continue
                val = _clean(r[i] if i < len(r) else None)
                if val is not None:
                    row[hcanon] = val
            key = norm_wav_key(row.get("wav_name"))
            if not key:
                continue
            cur = manifest.get(key, {})
            for k, v in row.items():
                if v is not None:
                    cur[k] = v
            manifest[key] = cur
    final: Dict[str, Dict[str, Any]] = {}
    for k, v in manifest.items():
        mapped = {
            "Wav_Name": v.get("wav_name"),
            "Csv_Name": v.get("csv_name"),
            "Conversation_ID": v.get("conversation_id"),
            "Start_Time": v.get("start_time"),
            "End_Time": v.get("end_time"),
            "Duration": v.get("duration"),
            "Xlsx_Name": v.get("xlsx_name"),
            "Consecutivo": v.get("consecutivo"),
            "Fecha_Ocurrencia": v.get("fecha_ocurrencia"),
            "Tiempo_Ocurrencia": v.get("tiempo_ocurrencia"),
            "Activo_Herope": v.get("activo_herope"),
            "Tipo_reporte": v.get("tipo_reporte"),
            "Tipo_Movimiento": v.get("tipo_movimiento"),
            "Denominación_causa E-Bitacora": v.get("denominación_causa e-bitacora"),
        }
        final[k] = {kk: vv for kk, vv in mapped.items() if vv is not None}
    return final