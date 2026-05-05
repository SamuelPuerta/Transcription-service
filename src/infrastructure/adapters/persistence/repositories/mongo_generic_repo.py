import asyncio
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from pymongo.asynchronous.collection import AsyncCollection
from pymongo.errors import (
    BulkWriteError,
    ConnectionFailure,
    DuplicateKeyError,
    NetworkTimeout,
    PyMongoError,
)
from pymongo.results import DeleteResult, InsertManyResult

from src.config.logger import logger
from src.infrastructure.exceptions.mongodb_exceptions import (
    DatabaseConnectionError,
    DatabaseDuplicateKeyError,
    DatabaseOperationError,
)

IndexType = Tuple[List[Tuple[str, int]], Dict[str, Any]]
BulkWriteParseResult = Tuple[int, Optional[int], List[Dict[str, Any]]]


@dataclass
class InsertManyState:
    retry_count: int
    remaining_docs: List[Dict[str, Any]]
    total_inserted: int = 0


class MongoGenericRepo:
    _MAX_RETRIES = 3
    _BASE_RETRY_DELAY = 0.5
    _indexes: Optional[List[IndexType]] = []

    def __init__(self, collection: AsyncCollection):
        self._collection = collection

    @property
    def _collection_name(self) -> str:
        return self._collection.name

    def _log_error(self, message: str, **context: Any) -> None:
        logger.error(message, context={"collection": self._collection_name, **context})

    def _log_warning(self, message: str, **context: Any) -> None:
        logger.warning(message, context={"collection": self._collection_name, **context})

    def _log_info(self, message: str, **context: Any) -> None:
        logger.info(message, context={"collection": self._collection_name, **context})

    @staticmethod
    def _set_timestamps(document: Dict[str, Any]) -> None:
        now = datetime.now(timezone.utc)
        document["created_at"] = now
        document["updated_at"] = now

    def _set_timestamps_many(self, documents: List[Dict[str, Any]]) -> None:
        for document in documents:
            self._set_timestamps(document)

    @staticmethod
    def _classify_and_raise(exc: Exception, *, operation: str, collection: str = "") -> None:
        if isinstance(exc, (ConnectionFailure, NetworkTimeout)):
            raise DatabaseConnectionError(
                f"Conexion perdida durante operacion '{operation}': {exc}",
                original_exception=exc,
            ) from exc
        if isinstance(exc, DuplicateKeyError):
            raise DatabaseDuplicateKeyError(
                collection=collection,
                key=str(exc.details.get("keyValue") if exc.details else exc),
                original_exception=exc,
            ) from exc
        raise DatabaseOperationError(
            operation=operation,
            error_detail=str(exc),
            original_exception=exc,
        ) from exc

    async def _create_index_in_collection(self, keys: List[Tuple[str, int]], options: Dict[str, Any]) -> None:
        await self._collection.create_index(keys, **options)

    async def _insert_one_in_collection(self, document: Dict[str, Any]) -> None:
        await self._collection.insert_one(document)

    async def _insert_many_in_collection(self, documents: List[Dict[str, Any]]) -> InsertManyResult:
        return await self._collection.insert_many(documents)

    async def _find_one_in_collection(self, query: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        return await self._collection.find_one(query)

    async def _find_many_in_collection(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        return await self._collection.find(query).to_list(length=None)

    async def _update_one_in_collection(self, query: Dict[str, Any], update: Dict[str, Any], *, upsert: bool = False):
        return await self._collection.update_one(query, update, upsert=upsert)

    async def _delete_one_in_collection(self, query: Dict[str, Any]) -> DeleteResult:
        return await self._collection.delete_one(query)

    def _raise_connection_error(self, exc: Exception, operation: str) -> None:
        self._classify_and_raise(exc, operation=operation)

    def _raise_operation_error(self, exc: Exception, operation: str) -> None:
        self._classify_and_raise(exc, operation=operation, collection=self._collection_name)

    @staticmethod
    def _parse_bulk_write_error(exc: BulkWriteError) -> BulkWriteParseResult:
        details = exc.details or {}
        inserted_count = details.get("nInserted", 0)
        write_errors = details.get("writeErrors", [])
        first_error_index = write_errors[0].get("index") if write_errors else None
        if first_error_index is None:
            return inserted_count, None, write_errors
        return first_error_index, first_error_index, write_errors

    def _compute_bulk_retry_wait_seconds(self, retry_count: int, write_errors: List[Dict[str, Any]]) -> float:
        retry_after_ms = self._find_retry_after_ms(write_errors)
        if retry_after_ms:
            return (retry_after_ms / 1000) + (self._BASE_RETRY_DELAY * retry_count)
        return self._BASE_RETRY_DELAY * (2 ** retry_count)

    def _find_retry_after_ms(self, write_errors: List[Dict[str, Any]]) -> Optional[int]:
        for write_error in write_errors:
            retry_after_ms = self._extract_retry_after_ms(write_error.get("errmsg", ""))
            if retry_after_ms is not None:
                return retry_after_ms
        return None

    async def _wait_bulk_retry(self, retry_count: int, write_errors: List[Dict[str, Any]]) -> None:
        wait_seconds = self._compute_bulk_retry_wait_seconds(retry_count, write_errors)
        self._log_warning(
            "Rate limit excedido en insert_many, reintentando",
            retry=retry_count,
            wait_seconds=round(wait_seconds, 2),
        )
        await asyncio.sleep(wait_seconds)

    async def _wait_pymongo_retry(self, retry_count: int) -> None:
        wait_seconds = self._BASE_RETRY_DELAY * (2 ** retry_count)
        self._log_warning(
            "Rate limit en insert_many (PyMongoError), reintentando",
            retry=retry_count,
            max_retries=self._MAX_RETRIES,
            wait_seconds=round(wait_seconds, 2),
        )
        await asyncio.sleep(wait_seconds)

    def _should_return_after_bulk_error(self, actual_inserted: int, remaining_docs: List[Dict[str, Any]]) -> bool:
        if actual_inserted == 0:
            return False
        return actual_inserted >= len(remaining_docs)

    @staticmethod
    def _slice_remaining_docs(remaining_docs: List[Dict[str, Any]], actual_inserted: int) -> List[Dict[str, Any]]:
        return remaining_docs[actual_inserted:]

    def _raise_bulk_rate_limit_exhausted(
        self,
        *,
        total_inserted: int,
        remaining_docs: List[Dict[str, Any]],
        actual_inserted: int,
        first_error_index: Optional[int],
        exc: BulkWriteError,
    ) -> None:
        self._log_error(
            "insert_many fallo tras max reintentos por rate limit",
            max_retries=self._MAX_RETRIES,
            total_inserted=total_inserted,
            failed_count=len(remaining_docs) - actual_inserted,
            first_error_index=first_error_index,
        )
        if total_inserted > 0:
            return
        raise DatabaseOperationError(
            operation="insert_many",
            error_detail=f"Rate limit excedido tras {self._MAX_RETRIES} reintentos",
            original_exception=exc,
        ) from exc

    def _is_retry_limit_reached(self, retry_count: int) -> bool:
        return retry_count >= self._MAX_RETRIES

    @staticmethod
    def _new_insert_many_state(documents: List[Dict[str, Any]]) -> InsertManyState:
        return InsertManyState(retry_count=0, remaining_docs=documents)

    async def _attempt_insert_many(self, state: InsertManyState) -> int:
        result = await self._insert_many_in_collection(state.remaining_docs)
        inserted_count = len(result.inserted_ids)
        state.total_inserted += inserted_count
        if state.retry_count > 0:
            self._log_info(
                "Documentos insertados tras reintentos",
                inserted=inserted_count,
                retries=state.retry_count,
            )
        return state.total_inserted

    async def _handle_insert_many_bulk_write_error(
        self,
        exc: BulkWriteError,
        state: InsertManyState,
    ) -> Optional[int]:
        actual_inserted, first_error_index, write_errors = self._parse_bulk_write_error(exc)
        self._log_partial_bulk_insert_warning(
            exc=exc,
            first_error_index=first_error_index,
        )
        state.total_inserted += actual_inserted

        if not self._is_rate_limit_error(exc):
            self._log_error("BulkWriteError no recuperable en insert_many", error=str(exc))
            self._raise_operation_error(exc, operation="insert_many")

        state.retry_count += 1
        if self._is_retry_limit_reached(state.retry_count):
            self._raise_bulk_rate_limit_exhausted(
                total_inserted=state.total_inserted,
                remaining_docs=state.remaining_docs,
                actual_inserted=actual_inserted,
                first_error_index=first_error_index,
                exc=exc,
            )
            if state.total_inserted > 0:
                return state.total_inserted

        await self._wait_bulk_retry(state.retry_count, write_errors)

        if actual_inserted == 0:
            self._log_error("Ningun documento insertado en el reintento", retry=state.retry_count)
            return None

        if self._should_return_after_bulk_error(actual_inserted, state.remaining_docs):
            return state.total_inserted

        state.remaining_docs = self._slice_remaining_docs(state.remaining_docs, actual_inserted)
        return None

    def _log_partial_bulk_insert_warning(
        self,
        *,
        exc: BulkWriteError,
        first_error_index: Optional[int],
    ) -> None:
        inserted_count = exc.details.get("nInserted", 0) if exc.details else 0
        if first_error_index is None or inserted_count == first_error_index:
            return
        self._log_warning(
            "Insercion parcial antes de error en bulk write",
            inserted=inserted_count,
            error_index=first_error_index,
        )

    async def _handle_insert_many_pymongo_rate_limit(
        self,
        exc: PyMongoError,
        state: InsertManyState,
    ) -> bool:
        if not self._is_rate_limit_error(exc):
            return False
        if self._is_retry_limit_reached(state.retry_count):
            return False

        state.retry_count += 1
        await self._wait_pymongo_retry(state.retry_count)
        return True

    async def create_index(self):
        for keys, options in self._indexes:
            try:
                await self._create_index_in_collection(keys, options)
            except PyMongoError as exc:
                self._log_error("Error creando indice en MongoDB", error=str(exc))
                self._raise_operation_error(exc, operation="create_index")

    async def insert_one(self, document: Dict[str, Any]):
        self._set_timestamps(document)
        try:
            await self._insert_one_in_collection(document)
        except DuplicateKeyError as exc:
            self._log_error("Clave duplicada al insertar documento", error=str(exc))
            self._raise_operation_error(exc, operation="insert_one")
        except (ConnectionFailure, NetworkTimeout) as exc:
            self._log_error("Error de conexion al insertar documento", error=str(exc))
            self._raise_connection_error(exc, operation="insert_one")
        except PyMongoError as exc:
            self._log_error("Error al insertar documento", error=str(exc))
            self._raise_operation_error(exc, operation="insert_one")

    async def insert_many(self, documents: List[Dict[str, Any]]):
        self._set_timestamps_many(documents)
        state = self._new_insert_many_state(documents)

        while not self._is_retry_limit_reached(state.retry_count):
            try:
                return await self._attempt_insert_many(state)
            except BulkWriteError as exc:
                result = await self._handle_insert_many_bulk_write_error(exc, state)
                if result is not None:
                    return result
                continue
            except (ConnectionFailure, NetworkTimeout) as exc:
                self._log_error("Error de conexion en insert_many", error=str(exc))
                self._raise_connection_error(exc, operation="insert_many")
            except PyMongoError as exc:
                if await self._handle_insert_many_pymongo_rate_limit(exc, state):
                    continue

                self._log_error("PyMongoError en insert_many", error=str(exc))
                self._raise_operation_error(exc, operation="insert_many")

        raise DatabaseOperationError(
            operation="insert_many",
            error_detail=f"insert_many fallo tras {self._MAX_RETRIES} reintentos",
        )

    async def find_one(self, query: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            return await self._find_one_in_collection(query)
        except (ConnectionFailure, NetworkTimeout) as exc:
            self._log_error("Error de conexion en find_one", error=str(exc))
            self._raise_connection_error(exc, operation="find_one")
        except PyMongoError as exc:
            self._log_error("Error en find_one", error=str(exc))
            self._raise_operation_error(exc, operation="find_one")

    async def find_many(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        try:
            return await self._find_many_in_collection(query)
        except (ConnectionFailure, NetworkTimeout) as exc:
            self._log_error("Error de conexion en find_many", error=str(exc))
            self._raise_connection_error(exc, operation="find_many")
        except PyMongoError as exc:
            self._log_error("Error en find_many", error=str(exc))
            self._raise_operation_error(exc, operation="find_many")

    async def update_one(self, query: Dict[str, Any], update: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            return await self._update_one_in_collection(query, update)
        except (ConnectionFailure, NetworkTimeout) as exc:
            self._log_error("Error de conexion en update_one", error=str(exc))
            self._raise_connection_error(exc, operation="update_one")
        except PyMongoError as exc:
            self._log_error("Error en update_one", error=str(exc))
            self._raise_operation_error(exc, operation="update_one")

    async def update_one_upsert(self, query: Dict[str, Any], update: Dict[str, Any]):
        try:
            return await self._update_one_in_collection(query, update, upsert=True)
        except DuplicateKeyError as exc:
            self._log_error("Clave duplicada en upsert", error=str(exc))
            self._raise_operation_error(exc, operation="update_one_upsert")
        except (ConnectionFailure, NetworkTimeout) as exc:
            self._log_error("Error de conexion en update_one_upsert", error=str(exc))
            self._raise_connection_error(exc, operation="update_one_upsert")
        except PyMongoError as exc:
            self._log_error("Error en update_one_upsert", error=str(exc))
            self._raise_operation_error(exc, operation="update_one_upsert")

    async def delete_one(self, query: Dict[str, Any]) -> Optional[int]:
        try:
            result = await self._delete_one_in_collection(query)
            return result.deleted_count
        except (ConnectionFailure, NetworkTimeout) as exc:
            self._log_error("Error de conexion en delete_one", error=str(exc))
            self._raise_connection_error(exc, operation="delete_one")
        except PyMongoError as exc:
            self._log_error("Error en delete_one", error=str(exc))
            self._raise_operation_error(exc, operation="delete_one")

    def _is_rate_limit_error(self, exc: PyMongoError) -> bool:
        if isinstance(exc, BulkWriteError):
            details = exc.details or {}
            for write_error in details.get("writeErrors", []):
                code = write_error.get("code")
                message = write_error.get("errmsg", "")
                if code == 16500:
                    return True
                if "429" in message or "TooManyRequests" in message:
                    return True
            return False

        message = str(exc)
        if "429" in message or "TooManyRequests" in message:
            return True
        if "Request rate os large" in message:
            return True
        return False

    def _extract_retry_after_ms(self, error_message: str) -> int | None:
        match = re.search(r"RetryAfterMs[=:](\d+)", error_message, re.IGNORECASE)
        if match:
            return int(match.group(1))
        return None
