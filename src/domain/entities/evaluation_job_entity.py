from dataclasses import dataclass

@dataclass(frozen=True)
class EvaluationJobEntity:
    batch_id: str
    file_id: str
    initiative_id: str
    correlation_id: str
