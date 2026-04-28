import pytest
from src.infrastructure.exceptions.base import InfrastructureException
from src.infrastructure.exceptions.ai_gateway_exceptions import (
    ExternalServiceError,
    ExternalTimeoutError,
    MappingError,
)
from src.infrastructure.exceptions.blob_storage_exceptions import (
    BlobStorageConnectionError,
    BlobStorageDownloadError,
    BlobStorageDeleteError,
)
from src.infrastructure.exceptions.mongodb_exceptions import (
    DatabaseConnectionError,
    DatabaseOperationError,
    DatabaseDuplicateKeyError,
)
from src.infrastructure.exceptions.service_bus_exceptions import (
    ServiceBusPublishError,
    ServiceBusConsumeError,
    ServiceBusMessageLockError,
)


# --- InfrastructureException base ---

@pytest.mark.unit
def test_infrastructure_exception_stores_message_code_and_original():
    original = ValueError("root cause")
    exc = InfrastructureException("something broke", error_code="test_code", original_exception=original)

    assert exc.message == "something broke"
    assert exc.error_code == "test_code"
    assert exc.original_exception is original


@pytest.mark.unit
def test_infrastructure_exception_str_includes_code_prefix():
    exc = InfrastructureException("broke", error_code="my_code")

    assert str(exc) == "[my_code] broke"


@pytest.mark.unit
def test_infrastructure_exception_str_without_code():
    exc = InfrastructureException("broke")

    assert str(exc) == "broke"


@pytest.mark.unit
def test_infrastructure_exception_is_exception_subclass():
    assert isinstance(InfrastructureException("e"), Exception)


# --- AI Gateway exceptions ---

@pytest.mark.unit
def test_external_service_error_has_correct_code():
    exc = ExternalServiceError("gateway down")

    assert exc.error_code == "external_service_error"
    assert "gateway down" in exc.message


@pytest.mark.unit
def test_external_timeout_error_has_correct_code():
    exc = ExternalTimeoutError("request timed out")

    assert exc.error_code == "external_timeout_error"
    assert "request timed out" in exc.message


@pytest.mark.unit
def test_mapping_error_has_correct_code():
    exc = MappingError("missing choices key")

    assert exc.error_code == "mapping_error"
    assert "missing choices key" in exc.message


@pytest.mark.unit
def test_ai_gateway_exceptions_store_original_exception():
    original = RuntimeError("root")
    exc = ExternalServiceError("error", original_exception=original)

    assert exc.original_exception is original


# --- Blob Storage exceptions ---

@pytest.mark.unit
def test_blob_storage_connection_error_has_correct_code():
    exc = BlobStorageConnectionError("auth failed")

    assert exc.error_code == "blob_storage_connection_error"
    assert "auth failed" in exc.message


@pytest.mark.unit
def test_blob_storage_download_error_embeds_container_and_path():
    exc = BlobStorageDownloadError("c1", "INIT/audio.wav", "not found")

    assert exc.error_code == "blob_storage_download_error"
    assert "c1" in exc.message
    assert "INIT/audio.wav" in exc.message
    assert "not found" in exc.message


@pytest.mark.unit
def test_blob_storage_delete_error_embeds_container_and_path():
    exc = BlobStorageDeleteError("c1", "INIT/audio.wav", "permission denied")

    assert exc.error_code == "blob_storage_delete_error"
    assert "c1" in exc.message
    assert "INIT/audio.wav" in exc.message


# --- MongoDB exceptions ---

@pytest.mark.unit
def test_database_connection_error_has_correct_code():
    exc = DatabaseConnectionError("connection refused")

    assert exc.error_code == "database_connection_error"
    assert "connection refused" in exc.message


@pytest.mark.unit
def test_database_operation_error_embeds_operation_and_detail():
    exc = DatabaseOperationError("find_one", "collection not found")

    assert exc.error_code == "database_operation_error"
    assert "find_one" in exc.message
    assert "collection not found" in exc.message


@pytest.mark.unit
def test_database_duplicate_key_error_embeds_collection_and_key():
    exc = DatabaseDuplicateKeyError("files_processing", "{'file_id': 'f1'}")

    assert exc.error_code == "database_duplicate_key_error"
    assert "files_processing" in exc.message
    assert "file_id" in exc.message


# --- Service Bus exceptions ---

@pytest.mark.unit
def test_service_bus_publish_error_has_correct_code():
    exc = ServiceBusPublishError("queue unavailable")

    assert exc.error_code == "service_bus_publish_error"
    assert "queue unavailable" in exc.message


@pytest.mark.unit
def test_service_bus_consume_error_has_correct_code():
    exc = ServiceBusConsumeError("connection dropped")

    assert exc.error_code == "service_bus_consume_error"
    assert "connection dropped" in exc.message


@pytest.mark.unit
def test_service_bus_message_lock_error_has_correct_code():
    exc = ServiceBusMessageLockError("lock expired after 60s")

    assert exc.error_code == "service_bus_message_lock_error"
    assert "lock expired" in exc.message


@pytest.mark.unit
def test_service_bus_exceptions_store_original_exception():
    original = RuntimeError("root cause")
    exc = ServiceBusPublishError("error", original_exception=original)

    assert exc.original_exception is original
