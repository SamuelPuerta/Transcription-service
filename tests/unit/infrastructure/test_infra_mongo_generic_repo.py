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
from src.infrastructure.adapters.persistence.repositories.mongo_generic_repo import (
    InsertManyState,
    MongoGenericRepo,
)
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


@pytest.mark.unit
def test_parse_bulk_write_error_returns_inserted_count_when_write_errors_missing():
    repo, _ = _repo()
    exc = BulkWriteError({"nInserted": 2})

    actual_inserted, first_error_index, write_errors = repo._parse_bulk_write_error(exc)

    assert actual_inserted == 2
    assert first_error_index is None
    assert write_errors == []


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


# --- insert_many ---

@pytest.mark.unit
@pytest.mark.asyncio
async def test_insert_many_returns_inserted_count_on_success():
    repo, collection = _repo()
    docs = [{"a": 1}, {"a": 2}]
    collection.insert_many.return_value = MagicMock(inserted_ids=["1", "2"])

    result = await repo.insert_many(docs)

    assert result == 2
    assert "created_at" in docs[0]
    assert "updated_at" in docs[0]
    collection.insert_many.assert_awaited_once_with(docs)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_insert_many_retries_bulk_rate_limit_and_continues_with_remaining_docs(monkeypatch):
    repo, collection = _repo()
    docs = [{"a": 1}, {"a": 2}, {"a": 3}]
    sleep_mock = AsyncMock()
    monkeypatch.setattr(
        "src.infrastructure.adapters.persistence.repositories.mongo_generic_repo.asyncio.sleep",
        sleep_mock,
    )
    bulk_exc = BulkWriteError(
        {
            "writeErrors": [{"code": 16500, "errmsg": "429 RetryAfterMs=1", "index": 1}],
            "nInserted": 1,
        }
    )
    collection.insert_many.side_effect = [bulk_exc, MagicMock(inserted_ids=["2", "3"])]

    result = await repo.insert_many(docs)

    assert result == 3
    assert collection.insert_many.await_count == 2
    first_call_docs = collection.insert_many.await_args_list[0].args[0]
    second_call_docs = collection.insert_many.await_args_list[1].args[0]
    assert len(first_call_docs) == 3
    assert len(second_call_docs) == 2
    assert second_call_docs[0]["a"] == 2
    sleep_mock.assert_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_insert_many_raises_database_operation_error_on_non_recoverable_bulk_write_error():
    repo, collection = _repo()
    docs = [{"a": 1}]
    collection.insert_many.side_effect = BulkWriteError(
        {
            "writeErrors": [{"code": 12000, "errmsg": "duplicate something", "index": 0}],
            "nInserted": 0,
        }
    )

    with pytest.raises(DatabaseOperationError):
        await repo.insert_many(docs)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_insert_many_retries_pymongo_rate_limit_error(monkeypatch):
    repo, collection = _repo()
    docs = [{"a": 1}]
    sleep_mock = AsyncMock()
    monkeypatch.setattr(
        "src.infrastructure.adapters.persistence.repositories.mongo_generic_repo.asyncio.sleep",
        sleep_mock,
    )
    collection.insert_many.side_effect = [
        PyMongoError("429 TooManyRequests"),
        MagicMock(inserted_ids=["1"]),
    ]

    result = await repo.insert_many(docs)

    assert result == 1
    assert collection.insert_many.await_count == 2
    sleep_mock.assert_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_insert_many_raises_when_bulk_rate_limit_retries_are_exhausted(monkeypatch):
    repo, collection = _repo()
    docs = [{"a": 1}]
    sleep_mock = AsyncMock()
    monkeypatch.setattr(
        "src.infrastructure.adapters.persistence.repositories.mongo_generic_repo.asyncio.sleep",
        sleep_mock,
    )
    bulk_exc = BulkWriteError(
        {
            "writeErrors": [{"code": 16500, "errmsg": "429 TooManyRequests", "index": 0}],
            "nInserted": 0,
        }
    )
    collection.insert_many.side_effect = [bulk_exc, bulk_exc, bulk_exc]

    with pytest.raises(DatabaseOperationError):
        await repo.insert_many(docs)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_insert_many_returns_total_inserted_when_max_retries_reached_after_partial_progress(monkeypatch):
    repo, collection = _repo()
    docs = [{"a": 1}, {"a": 2}]
    sleep_mock = AsyncMock()
    monkeypatch.setattr(
        "src.infrastructure.adapters.persistence.repositories.mongo_generic_repo.asyncio.sleep",
        sleep_mock,
    )
    bulk_partial = BulkWriteError(
        {
            "writeErrors": [{"code": 16500, "errmsg": "429 TooManyRequests", "index": 1}],
            "nInserted": 1,
        }
    )
    bulk_zero = BulkWriteError(
        {
            "writeErrors": [{"code": 16500, "errmsg": "429 TooManyRequests", "index": 0}],
            "nInserted": 0,
        }
    )
    collection.insert_many.side_effect = [bulk_partial, bulk_zero, bulk_zero]

    result = await repo.insert_many(docs)

    assert result == 1
    assert collection.insert_many.await_count == 3


@pytest.mark.unit
@pytest.mark.asyncio
async def test_insert_many_returns_when_bulk_error_reports_all_remaining_as_inserted(monkeypatch):
    repo, collection = _repo()
    docs = [{"a": 1}]
    sleep_mock = AsyncMock()
    monkeypatch.setattr(
        "src.infrastructure.adapters.persistence.repositories.mongo_generic_repo.asyncio.sleep",
        sleep_mock,
    )
    collection.insert_many.side_effect = BulkWriteError(
        {
            "writeErrors": [{"code": 16500, "errmsg": "429 TooManyRequests", "index": 1}],
            "nInserted": 0,
        }
    )

    result = await repo.insert_many(docs)

    assert result == 1


@pytest.mark.unit
@pytest.mark.asyncio
async def test_insert_many_raises_database_connection_error_on_connection_failure():
    repo, collection = _repo()
    collection.insert_many.side_effect = ConnectionFailure("refused")

    with pytest.raises(DatabaseConnectionError):
        await repo.insert_many([{"a": 1}])


@pytest.mark.unit
@pytest.mark.asyncio
async def test_insert_many_raises_database_operation_error_on_non_rate_limit_pymongo_error():
    repo, collection = _repo()
    collection.insert_many.side_effect = PyMongoError("generic")

    with pytest.raises(DatabaseOperationError):
        await repo.insert_many([{"a": 1}])


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_insert_many_pymongo_rate_limit_returns_false_for_non_rate_error():
    repo, _ = _repo()
    state = InsertManyState(retry_count=0, remaining_docs=[])

    handled = await repo._handle_insert_many_pymongo_rate_limit(PyMongoError("generic"), state)

    assert handled is False


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_insert_many_pymongo_rate_limit_returns_false_when_retry_limit_reached():
    repo, _ = _repo()
    state = InsertManyState(retry_count=repo._MAX_RETRIES, remaining_docs=[])

    handled = await repo._handle_insert_many_pymongo_rate_limit(PyMongoError("429 TooManyRequests"), state)

    assert handled is False


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


@pytest.mark.unit
@pytest.mark.asyncio
async def test_find_many_raises_database_operation_error_on_pymongo_error():
    repo, collection = _repo()
    collection.find = MagicMock(side_effect=PyMongoError("generic"))

    with pytest.raises(DatabaseOperationError):
        await repo.find_many({})


# --- update_one ---

@pytest.mark.unit
@pytest.mark.asyncio
async def test_update_one_calls_collection_update_one_with_query_and_update():
    repo, collection = _repo()
    collection.update_one.return_value = MagicMock(modified_count=1)

    await repo.update_one({"_id": "1"}, {"$set": {"field": "new"}})

    collection.update_one.assert_awaited_once_with(
        {"_id": "1"},
        {"$set": {"field": "new"}},
        upsert=False,
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_update_one_raises_database_connection_error_on_connection_failure():
    repo, collection = _repo()
    collection.update_one.side_effect = ConnectionFailure("refused")

    with pytest.raises(DatabaseConnectionError):
        await repo.update_one({}, {})


@pytest.mark.unit
@pytest.mark.asyncio
async def test_update_one_raises_database_operation_error_on_pymongo_error():
    repo, collection = _repo()
    collection.update_one.side_effect = PyMongoError("generic")

    with pytest.raises(DatabaseOperationError):
        await repo.update_one({}, {})


# --- update_one_upsert ---

@pytest.mark.unit
@pytest.mark.asyncio
async def test_update_one_upsert_calls_collection_with_upsert_true():
    repo, collection = _repo()

    await repo.update_one_upsert({"_id": "1"}, {"$set": {"field": "new"}})

    collection.update_one.assert_awaited_once_with(
        {"_id": "1"},
        {"$set": {"field": "new"}},
        upsert=True,
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_update_one_upsert_raises_database_duplicate_key_error_on_duplicate():
    repo, collection = _repo()
    collection.update_one.side_effect = DuplicateKeyError("dup", details={"keyValue": {"_id": "1"}})

    with pytest.raises(DatabaseDuplicateKeyError):
        await repo.update_one_upsert({}, {})


@pytest.mark.unit
@pytest.mark.asyncio
async def test_update_one_upsert_raises_database_connection_error_on_connection_failure():
    repo, collection = _repo()
    collection.update_one.side_effect = ConnectionFailure("refused")

    with pytest.raises(DatabaseConnectionError):
        await repo.update_one_upsert({}, {})


@pytest.mark.unit
@pytest.mark.asyncio
async def test_update_one_upsert_raises_database_operation_error_on_pymongo_error():
    repo, collection = _repo()
    collection.update_one.side_effect = PyMongoError("generic")

    with pytest.raises(DatabaseOperationError):
        await repo.update_one_upsert({}, {})


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


@pytest.mark.unit
@pytest.mark.asyncio
async def test_delete_one_raises_database_operation_error_on_pymongo_error():
    repo, collection = _repo()
    collection.delete_one.side_effect = PyMongoError("generic")

    with pytest.raises(DatabaseOperationError):
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


@pytest.mark.unit
def test_is_rate_limit_error_returns_true_for_request_rate_os_large_message():
    repo, _ = _repo()
    exc = PyMongoError("Request rate os large")

    assert repo._is_rate_limit_error(exc) is True
