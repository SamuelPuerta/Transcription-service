import pytest
from io import BytesIO
from unittest.mock import AsyncMock, MagicMock
from azure.core.exceptions import ClientAuthenticationError, ServiceRequestError, ResourceNotFoundError
from src.infrastructure.adapters.blob_storage.blob_storage_adapter import BlobStorageAdapter
from src.infrastructure.exceptions.blob_storage_exceptions import (
    BlobStorageConnectionError,
    BlobStorageDownloadError,
)
from src.domain.entities.blob_file_reference_entity import BlobFileReferenceEntity


def _ref(**kw):
    base = dict(
        account_name="myaccount",
        account_key="mykey",
        container_name="c1",
        blob_path="INIT/2026-01-28/audio.wav",
    )
    base.update(kw)
    return BlobFileReferenceEntity(**base)


def _make_service_client(download_data=b"audio content", raise_on_get_blob=None, raise_on_download=None):
    downloader = AsyncMock()
    if raise_on_download:
        downloader.readall.side_effect = raise_on_download
    else:
        downloader.readall.return_value = download_data

    blob_client = AsyncMock()
    if raise_on_get_blob:
        blob_client.download_blob.side_effect = raise_on_get_blob
    else:
        blob_client.download_blob.return_value = downloader

    service_client = MagicMock()
    service_client.get_blob_client.return_value = blob_client
    service_client.__aenter__ = AsyncMock(return_value=service_client)
    service_client.__aexit__ = AsyncMock(return_value=False)
    return service_client


@pytest.mark.unit
@pytest.mark.asyncio
async def test_download_file_raises_when_account_name_is_empty():
    adapter = BlobStorageAdapter()

    with pytest.raises(BlobStorageConnectionError, match="account_name"):
        await adapter.download_file(_ref(account_name=""))


@pytest.mark.unit
@pytest.mark.asyncio
async def test_download_file_raises_when_account_key_is_empty():
    adapter = BlobStorageAdapter()

    with pytest.raises(BlobStorageConnectionError, match="account_key"):
        await adapter.download_file(_ref(account_key=""))


@pytest.mark.unit
@pytest.mark.asyncio
async def test_download_file_raises_when_container_name_is_empty():
    adapter = BlobStorageAdapter()

    with pytest.raises(BlobStorageConnectionError, match="container_name"):
        await adapter.download_file(_ref(container_name=""))


@pytest.mark.unit
@pytest.mark.asyncio
async def test_download_file_raises_when_blob_path_is_empty():
    adapter = BlobStorageAdapter()

    with pytest.raises(BlobStorageConnectionError, match="blob_path"):
        await adapter.download_file(_ref(blob_path=""))


@pytest.mark.unit
@pytest.mark.asyncio
async def test_download_file_returns_byte_stream_on_success():
    adapter = BlobStorageAdapter()
    service_client = _make_service_client(download_data=b"audio content")
    adapter._create_blob_service_client = MagicMock(return_value=service_client)

    result = await adapter.download_file(_ref())

    assert isinstance(result, BytesIO)
    assert result.read() == b"audio content"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_download_file_raises_blob_storage_connection_error_on_auth_failure():
    adapter = BlobStorageAdapter()
    service_client = _make_service_client(raise_on_get_blob=ClientAuthenticationError("auth failed"))
    adapter._create_blob_service_client = MagicMock(return_value=service_client)

    with pytest.raises(BlobStorageConnectionError):
        await adapter.download_file(_ref())


@pytest.mark.unit
@pytest.mark.asyncio
async def test_download_file_raises_blob_storage_download_error_on_not_found():
    adapter = BlobStorageAdapter()
    service_client = _make_service_client(raise_on_download=ResourceNotFoundError("blob not found"))
    adapter._create_blob_service_client = MagicMock(return_value=service_client)

    with pytest.raises(BlobStorageDownloadError):
        await adapter.download_file(_ref())


@pytest.mark.unit
@pytest.mark.asyncio
async def test_download_file_raises_blob_storage_connection_error_on_network_failure():
    adapter = BlobStorageAdapter()
    service_client = _make_service_client(raise_on_get_blob=ServiceRequestError("network error"))
    adapter._create_blob_service_client = MagicMock(return_value=service_client)

    with pytest.raises(BlobStorageConnectionError):
        await adapter.download_file(_ref())
