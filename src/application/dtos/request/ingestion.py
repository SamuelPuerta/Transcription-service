from dataclasses import dataclass

@dataclass(frozen=True)
class StorageEventRequestDTO:
    batch_id: str
    initiative_id: str
    blob_url: str
    file_name: str
    container_name: str
    correlation_id: str