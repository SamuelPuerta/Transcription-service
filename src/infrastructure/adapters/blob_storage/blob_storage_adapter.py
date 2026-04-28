from io import BytesIO
from azure.core.credentials import AzureNamedKeyCredential
from azure.core.exceptions import (
    ClientAuthenticationError,
    HttpResponseError,
    ResourceExistsError,
    ResourceNotFoundError,
    ServiceRequestError,
)
from azure.storage.blob.aio import BlobServiceClient
from src.config.logger import logger
from src.domain.entities.blob_file_reference_entity import BlobFileReferenceEntity
from src.domain.ports.infrastructure.blob_storage.blob_storage import BlobStorage
from src.infrastructure.exceptions.blob_storage_exceptions import (
    BlobStorageConnectionError,
    BlobStorageDeleteError,
    BlobStorageDownloadError,
)

class BlobStorageAdapter(BlobStorage):
    def _create_blob_service_client(
        self,
        account_name: str,
        account_key: str,
    ) -> BlobServiceClient:
        account_url = f"https://{account_name}.blob.core.windows.net"
        credential = AzureNamedKeyCredential(name=account_name, key=account_key)
        return BlobServiceClient(account_url=account_url, credential=credential)

    @staticmethod
    def _classify_and_raise(
        exc: Exception,
        *,
        container: str,
        path: str,
        operation: str,
    ) -> None:
        if isinstance(exc, ClientAuthenticationError):
            raise BlobStorageConnectionError(
                f"Credenciales invalidas o sin permisos en contenedor '{container}'",
                original_exception=exc,
            ) from exc
        if isinstance(exc, ServiceRequestError):
            raise BlobStorageConnectionError(
                f"Error de red al conectar con Azure Blob Storage: {exc}",
                original_exception=exc,
            ) from exc
        if operation == "delete":
            raise BlobStorageDeleteError(container, path, str(exc), exc) from exc
        raise BlobStorageDownloadError(container, path, str(exc), exc) from exc

    async def download_file(self, reference: BlobFileReferenceEntity) -> BytesIO:
        if not reference.account_name:
            raise BlobStorageConnectionError("account_name es requerido")
        if not reference.account_key:
            raise BlobStorageConnectionError("account_key es requerido")
        if not reference.container_name:
            raise BlobStorageConnectionError("container_name es requerido")
        if not reference.blob_path:
            raise BlobStorageConnectionError("blob_path es requerido")
        try:
            service = self._create_blob_service_client(
                account_name=reference.account_name,
                account_key=reference.account_key,
            )
            async with service:
                blob_client = service.get_blob_client(
                    container=reference.container_name,
                    blob=reference.blob_path,
                )
                downloader = await blob_client.download_blob()
                data = await downloader.readall()
                file_buffer = BytesIO(data)
                file_buffer.seek(0)
                logger.info("Archivo descargado de Blob Storage", context={"container": reference.container_name, "blob_path": reference.blob_path})
                return file_buffer
        except (
            ResourceNotFoundError,
            ResourceExistsError,
            ClientAuthenticationError,
            ServiceRequestError,
            HttpResponseError,
        ) as e:
            logger.error("Error descargando archivo de Blob Storage", context={"container": reference.container_name, "blob_path": reference.blob_path, "error": str(e)})
            self._classify_and_raise(
                e,
                container=reference.container_name,
                path=reference.blob_path,
                operation="download",
            )
        except Exception as e:
            logger.error("Error inesperado descargando archivo de Blob Storage", context={"container": reference.container_name, "blob_path": reference.blob_path, "error": str(e)})
            self._classify_and_raise(
                e,
                container=reference.container_name,
                path=reference.blob_path,
                operation="download",
            )
