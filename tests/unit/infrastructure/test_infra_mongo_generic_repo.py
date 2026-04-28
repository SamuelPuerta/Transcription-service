import pytest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock
from pymongo.errors import (
    BulkWriteError,
    ConnectionFailure,
    DuplicateKeyError,
    NetworkTimeout,
    PyMongoError,
)
from src.infrastructure.adapters.persistence.repositories.mongo_generic_repo import MongoGenericRepo
from src.infrastructure.exceptions.mongodb_exceptions import (
    DatabaseConnectionError,
    DatabaseDuplicateKeyError,
    DatabaseOperationError,
)


def _repo():
    collection = AsyncMock()
    collection.name = "test_collection"
    return MongoGenericRepo(collection), collection


# --- _classify_and_raise ---

@pytest.mark.unit
def test_classify_and_raise_connection_failure_becomes_database_connection_error():
    repo, _ = _repo()

    with pytest.raises(DatabaseConnectionError):
        repo._classify_and_raise(ConnectionFailure("refused"), operation="find_one")


@pytest.mark.unit
def test_classify_and_raise_network_timeout_becomes_database_connection_error():
    repo, _ = _repo()

    with pytest.raises(DatabaseConnectionError):
        repo._classify_and_raise(NetworkTimeout("timeout"), operation="find_one")


@pytest.mark.unit
def test_classify_and_raise_duplicate_key_error_becomes_database_duplicate_key_error():
    repo, _ = _repo()
    exc = DuplicateKeyError("E11000", details={"keyValue": {"_id": "x"}})

    with pytest.raises(DatabaseDuplicateKeyError):
        repo._classify_and_raise(exc, operation="insert_one", collection="col")


@pytest.mark.unit
def test_classify_and_raise_generic_pymongo_error_becomes_database_operation_error():
    repo, _ = _repo()

    with pytest.raises(DatabaseOperationError):
        repo._classify_and_raise(PyMongoError("generic"), operation="find_one")


# --- _is_rate_limit_error ---

@pytest.mark.unit
def test_is_rate_limit_error_returns_true_for_bulk_write_error_with_code_16500():
    repo, _ = _repo()
    exc = BulkWriteError({"writeErrors": [{"code": 16500, "errmsg": "rate limit"}], "nInserted": 0})

    assert repo._is_rate_limit_error(exc) is True


@pytest.mark.unit
def test_is_rate_limit_error_returns_true_for_bulk_write_error_with_429_in_message():
    repo, _ = _repo()
    exc = BulkWriteError({"writeErrors": [{"code": 999, "errmsg": "429 TooManyRequests"}], "nInserted": 0})

    assert repo._is_rate_limit_error(exc) is True


@pytest.mark.unit
def test_is_rate_limit_error_returns_true_for_pymongo_error_with_429_in_message():
    repo, _ = _repo()
    exc = PyMongoError("Error 429 TooManyRequests exceeded")

    assert repo._is_rate_limit_error(exc) is True


@pytest.mark.unit
def test_is_rate_limit_error_returns_false_for_unrelated_error():
    repo, _ = _repo()
    exc = PyMongoError("Collection not found")

    assert repo._is_rate_limit_error(exc) is False


# --- _extract_retry_after_ms ---

@pytest.mark.unit
def test_extract_retry_after_ms_parses_value_from_error_message():
    repo, _ = _repo()

    result = repo._extract_retry_after_ms("RetryAfterMs=1500 TooManyRequests")

    assert result == 1500


@pytest.mark.unit
def test_extract_retry_after_ms_returns_none_when_not_present():
    repo, _ = _repo()

    result = repo._extract_retry_after_ms("Generic error with no retry info")

    assert result is None


# --- insert_one ---

@pytest.mark.unit
@pytest.mark.asyncio
async def test_insert_one_sets_created_at_and_updated_at_on_document():
    repo, collection = _repo()
    doc = {"field": "value"}

    await repo.insert_one(doc)

    assert "created_at" in doc
    assert "updated_at" in doc
    assert isinstance(doc["created_at"], datetime)
    collection.insert_one.assert_awaited_once_with(doc)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_insert_one_raises_database_duplicate_key_error_on_duplicate():
    repo, collection = _repo()
    collection.insert_one.side_effect = DuplicateKeyError("E11000", details={"keyValue": {}})

    with pytest.raises(DatabaseDuplicateKeyError):
        await repo.insert_one({"field": "value"})


@pytest.mark.unit
@pytest.mark.asyncio
async def test_insert_one_raises_database_connection_error_on_connection_failure():
    repo, collection = _repo()
    collection.insert_one.side_effect = ConnectionFailure("refused")

    with pytest.raises(DatabaseConnectionError):
        await repo.insert_one({"field": "value"})


@pytest.mark.unit
@pytest.mark.asyncio
async def test_insert_one_raises_database_operation_error_on_pymongo_error():
    repo, collection = _repo()
    collection.insert_one.side_effect = PyMongoError("unknown error")

    with pytest.raises(DatabaseOperationError):
        await repo.insert_one({"field": "value"})


# --- find_one ---

@pytest.mark.unit
@pytest.mark.asyncio
async def test_find_one_returns_document_when_found():
    repo, collection = _repo()
    collection.find_one.return_value = {"_id": "1", "field": "value"}

    result = await repo.find_one({"field": "value"})

    assert result == {"_id": "1", "field": "value"}
    collection.find_one.assert_awaited_once_with({"field": "value"})


@pytest.mark.unit
@pytest.mark.asyncio
async def test_find_one_returns_none_when_not_found():
    repo, collection = _repo()
    collection.find_one.return_value = None

    result = await repo.find_one({"field": "missing"})

    assert result is None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_find_one_raises_database_connection_error_on_connection_failure():
    repo, collection = _repo()
    collection.find_one.side_effect = ConnectionFailure("refused")

    with pytest.raises(DatabaseConnectionError):
        await repo.find_one({"field": "value"})


@pytest.mark.unit
@pytest.mark.asyncio
async def test_find_one_raises_database_operation_error_on_pymongo_error():
    repo, collection = _repo()
    collection.find_one.side_effect = PyMongoError("generic")

    with pytest.raises(DatabaseOperationError):
        await repo.find_one({})


# --- find_many ---

@pytest.mark.unit
@pytest.mark.asyncio
async def test_find_many_returns_list_of_documents():
    repo, collection = _repo()
    cursor = MagicMock()
    cursor.to_list = AsyncMock(return_value=[{"a": 1}, {"a": 2}])
    collection.find = MagicMock(return_value=cursor)

    result = await repo.find_many({"a": {"$gt": 0}})

    assert result == [{"a": 1}, {"a": 2}]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_find_many_raises_database_connection_error_on_connection_failure():
    repo, collection = _repo()
    collection.find = MagicMock(side_effect=ConnectionFailure("refused"))

    with pytest.raises(DatabaseConnectionError):
        await repo.find_many({})


# --- update_one ---

@pytest.mark.unit
@pytest.mark.asyncio
async def test_update_one_calls_collection_update_one_with_query_and_update():
    repo, collection = _repo()
    collection.update_one.return_value = MagicMock(modified_count=1)

    await repo.update_one({"_id": "1"}, {"$set": {"field": "new"}})

    collection.update_one.assert_awaited_once_with({"_id": "1"}, {"$set": {"field": "new"}})


@pytest.mark.unit
@pytest.mark.asyncio
async def test_update_one_raises_database_connection_error_on_connection_failure():
    repo, collection = _repo()
    collection.update_one.side_effect = ConnectionFailure("refused")

    with pytest.raises(DatabaseConnectionError):
        await repo.update_one({}, {})


# --- delete_one ---

@pytest.mark.unit
@pytest.mark.asyncio
async def test_delete_one_returns_deleted_count():
    repo, collection = _repo()
    delete_result = MagicMock()
    delete_result.deleted_count = 1
    collection.delete_one.return_value = delete_result

    result = await repo.delete_one({"_id": "1"})

    assert result == 1
    collection.delete_one.assert_awaited_once_with({"_id": "1"})


@pytest.mark.unit
@pytest.mark.asyncio
async def test_delete_one_raises_database_connection_error_on_connection_failure():
    repo, collection = _repo()
    collection.delete_one.side_effect = ConnectionFailure("refused")

    with pytest.raises(DatabaseConnectionError):
        await repo.delete_one({})


# --- create_index ---

@pytest.mark.unit
@pytest.mark.asyncio
async def test_create_index_calls_collection_create_index_for_each_index():
    repo, collection = _repo()
    repo._indexes = [
        ([("file_id", 1)], {"unique": True, "name": "uq_file_id"}),
        ([("batch_id", 1)], {"name": "idx_batch_id"}),
    ]

    await repo.create_index()

    assert collection.create_index.await_count == 2


@pytest.mark.unit
@pytest.mark.asyncio
async def test_create_index_raises_database_operation_error_on_pymongo_error():
    repo, collection = _repo()
    repo._indexes = [([("field", 1)], {"name": "idx_field"})]
    collection.create_index.side_effect = PyMongoError("index error")

    with pytest.raises(DatabaseOperationError):
        await repo.create_index()
