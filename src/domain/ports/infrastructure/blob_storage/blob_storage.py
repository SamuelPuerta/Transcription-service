from io import BytesIO
from typing import Protocol
from src.domain.entities.blob_file_reference_entity import BlobFileReferenceEntity

class BlobStorage(Protocol):
    async def download_file(self, reference: BlobFileReferenceEntity) -> BytesIO: ...
