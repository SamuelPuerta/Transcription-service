from dataclasses import dataclass

@dataclass(frozen=True)
class ProcessEvaluationJobRequestDTO:
    file_id: str
    batch_id: str
    initiative_id: str
    correlation_id: str
    evaluation_json: dict | None = None
