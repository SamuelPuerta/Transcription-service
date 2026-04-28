from src.infrastructure.exceptions.base import InfrastructureException

class BlobStorageConnectionError(InfrastructureException):
    def __init__(self, error_detail: str, original_exception: Exception = None):
        super().__init__(
            message=f"Error de conexión con Azure Blob Storage: {error_detail}",
            error_code="blob_storage_connection_error",
            original_exception=original_exception,
        )

class BlobStorageDownloadError(InfrastructureException):
    def __init__(self, container: str, path: str, error_detail: str, original_exception: Exception = None):
        super().__init__(
            message=f"Error al descargar el archivo '{path}' del contenedor '{container}': {error_detail}",
            error_code="blob_storage_download_error",
            original_exception=original_exception,
        )

class BlobStorageDeleteError(InfrastructureException):
    def __init__(self, container: str, path: str, error_detail: str, original_exception: Exception = None):
        super().__init__(
            message=f"Error al eliminar el blob '{path}' del contenedor '{container}': {error_detail}",
            error_code="blob_storage_delete_error",
            original_exception=original_exception,
        )
