from dataclasses import dataclass

@dataclass(frozen=True)
class BlobFileReferenceEntity:
    account_name: str
    account_key: str
    container_name: str
    blob_path: str
