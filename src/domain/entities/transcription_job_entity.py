from dataclasses import dataclass

@dataclass(frozen=True)
class TranscriptionJobEntity:
    batch_id: str
    blob_url: str
    file_name: str
    file_id: str
    initiative_id: str
    storage_container: str
    transcription_id: str
    correlation_id: str
