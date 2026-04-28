from src.domain.entities.blob_file_reference_entity import BlobFileReferenceEntity

def to_blob_file_reference_entity(
    *,
    account_name: str,
    account_key: str,
    container_name: str,
    blob_path: str,
) -> BlobFileReferenceEntity:
    return BlobFileReferenceEntity(
        account_name=account_name,
        account_key=account_key,
        container_name=container_name,
        blob_path=blob_path,
    )
