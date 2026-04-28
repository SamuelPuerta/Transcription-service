from dataclasses import dataclass
from typing import Optional
from src.domain.entities.files_processing_entity import EvaluationResult, OperativeEvent

@dataclass(frozen=True)
class FilesXlsxEnrichmentEntity:
    csv_name: Optional[str] = None
    xlsx_name: Optional[str] = None
    conversation_id: Optional[str] = None
    consecutive: Optional[str] = None
    operative_event: Optional[OperativeEvent] = None
    evaluation_result: Optional[EvaluationResult] = None
