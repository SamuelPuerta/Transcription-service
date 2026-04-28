from dataclasses import dataclass
from typing import Optional
from src.domain.entities.files_processing_entity import EvaluationResult

@dataclass(frozen=True)
class FilesEvaluationEnrichmentEntity:
    cct_engineer: Optional[str] = None
    se_operator: Optional[str] = None
    substation: Optional[str] = None
    engineer_score: Optional[float] = 0.0
    operator_score: Optional[float] = 0.0
    evaluation_result: Optional[EvaluationResult] = None
