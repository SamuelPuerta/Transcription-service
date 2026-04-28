from dataclasses import dataclass

@dataclass(frozen=True)
class ManifestEnrichmentRequestDTO:
    file_id: str
    batch_id: str
    initiative_id: str
    file_name: str
    xlsx_name: str

@dataclass(frozen=True)
class TranscriptionJobRequestDTO:
    file_id: str
    batch_id: str
    initiative_id: str
    transcription_id: str
    blob_url: str
    file_name: str
    correlation_id: str

@dataclass(frozen=True)
class QueueEvaluationJobRequestDTO:
    file_id: str
    batch_id: str
    initiative_id: str
    correlation_id: str