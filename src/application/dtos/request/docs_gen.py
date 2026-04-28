from dataclasses import dataclass

@dataclass(frozen=True)
class CompleteBatchRequestDTO:
    batch_id: str
    initiative_id: str
    correlation_id: str