from dataclasses import dataclass

@dataclass(frozen=True)
class DocumentGenerationJobEntity:
    batch_id: str
    initiative_id: str
    correlation_id: str
