import asyncio
from datetime import datetime, timezone
import re
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

class MongoGenericRepo:
    _MAX_RETRIES = 3
    _BASE_RETRY_DELAY = 0.5
    _indexes: Optional[List[IndexType]] = []

    def __init__(self, collection: AsyncCollection):
        self._collection = collection

    @staticmethod
    def _classify_and_raise(exc: Exception, *, operation: str, collection: str = "") -> None:
        if isinstance(exc, (ConnectionFailure, NetworkTimeout)):
            raise DatabaseConnectionError(
                f"Conexión perdida durante operación '{operation}': {exc}",
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

    async def create_index(self):
        for keys, kwargs in self._indexes:
            try:
                await self._collection.create_index(keys, **kwargs)
            except PyMongoError as e:
                logger.error("Error creando indice en MongoDB", context={"collection": self._collection.name, "error": str(e)})
                self._classify_and_raise(e, operation="create_index", collection=self._collection.name)

    async def insert_one(self, document: Dict[str, Any]):
        document["created_at"] = datetime.now(timezone.utc)
        document["updated_at"] = datetime.now(timezone.utc)
        try:
            await self._collection.insert_one(document)
        except DuplicateKeyError as e:
            logger.error("Clave duplicada al insertar documento", context={"collection": self._collection.name, "error": str(e)})
            self._classify_and_raise(e, operation="insert_one", collection=self._collection.name)
        except (ConnectionFailure, NetworkTimeout) as e:
            logger.error("Error de conexion al insertar documento", context={"collection": self._collection.name, "error": str(e)})
            self._classify_and_raise(e, operation="insert_one")
        except PyMongoError as e:
            logger.error("Error al insertar documento", context={"collection": self._collection.name, "error": str(e)})
            self._classify_and_raise(e, operation="insert_one", collection=self._collection.name)

    async def insert_many(self, documents: List[Dict[str, Any]]):
        for document in documents:
            document["created_at"] = datetime.now(timezone.utc)
            document["updated_at"] = datetime.now(timezone.utc)

        retry_count = 0
        remaining_docs = documents
        total_inserted = 0

        while retry_count < self._MAX_RETRIES:
            try:
                res: InsertManyResult = await self._collection.insert_many(remaining_docs)
                inserted_count = len(res.inserted_ids)
                total_inserted += inserted_count
                if retry_count > 0:
                    logger.info("Documentos insertados tras reintentos", context={"collection": self._collection.name, "inserted": inserted_count, "retries": retry_count})
                return total_inserted
            except BulkWriteError as e:
                inserted_count = e.details.get("nInserted", 0) if e.details else 0
                write_errors = e.details.get("writeErrors", []) if e.details else []
                first_error_index = None
                if write_errors:
                    first_error_index = write_errors[0].get("index", None)
                if first_error_index is not None:
                    if inserted_count != first_error_index:
                        logger.warning("Insercion parcial antes de error en bulk write", context={"collection": self._collection.name, "inserted": inserted_count, "error_index": first_error_index})
                    actual_inserted = first_error_index
                else:
                    actual_inserted = inserted_count
                total_inserted += actual_inserted

                if self._is_rate_limit_error(e):
                    retry_count += 1
                    if retry_count >= self._MAX_RETRIES:
                        logger.error("insert_many fallo tras max reintentos por rate limit", context={"collection": self._collection.name, "max_retries": self._MAX_RETRIES, "total_inserted": total_inserted, "failed_count": len(remaining_docs) - actual_inserted, "first_error_index": first_error_index})
                        if total_inserted > 0:
                            return total_inserted
                        raise DatabaseOperationError(
                            operation="insert_many",
                            error_detail=f"Rate limit excedido tras {self._MAX_RETRIES} reintentos",
                            original_exception=e,
                        ) from e

                    retry_after_ms = None
                    for write_error in write_errors:
                        retry_after_ms = self._extract_retry_after_ms(write_error.get("errmsg", ""))
                        if retry_after_ms is not None:
                            break
                    wait_time = (
                        (retry_after_ms / 1000) + (self._BASE_RETRY_DELAY * retry_count)
                        if retry_after_ms
                        else self._BASE_RETRY_DELAY * (2 ** retry_count)
                    )
                    logger.warning("Rate limit excedido en insert_many, reintentando", context={"collection": self._collection.name, "retry": retry_count, "wait_seconds": round(wait_time, 2)})
                    await asyncio.sleep(wait_time)

                    if actual_inserted > 0 and actual_inserted < len(remaining_docs):
                        remaining_docs = remaining_docs[actual_inserted:]
                    elif actual_inserted == 0:
                        logger.error("Ningun documento insertado en el reintento", context={"collection": self._collection.name, "retry": retry_count})
                    else:
                        return total_inserted
                    continue
                else:
                    logger.error("BulkWriteError no recuperable en insert_many", context={"collection": self._collection.name, "error": str(e)})
                    self._classify_and_raise(e, operation="insert_many", collection=self._collection.name)
            except (ConnectionFailure, NetworkTimeout) as e:
                logger.error("Error de conexion en insert_many", context={"collection": self._collection.name, "error": str(e)})
                self._classify_and_raise(e, operation="insert_many")
            except PyMongoError as e:
                if self._is_rate_limit_error(e) and retry_count < self._MAX_RETRIES:
                    retry_count += 1
                    wait_time = self._BASE_RETRY_DELAY * (2 ** retry_count)
                    logger.warning("Rate limit en insert_many (PyMongoError), reintentando", context={"collection": self._collection.name, "retry": retry_count, "max_retries": self._MAX_RETRIES, "wait_seconds": round(wait_time, 2)})
                    await asyncio.sleep(wait_time)
                    continue
                logger.error("PyMongoError en insert_many", context={"collection": self._collection.name, "error": str(e)})
                self._classify_and_raise(e, operation="insert_many", collection=self._collection.name)

        raise DatabaseOperationError(
            operation="insert_many",
            error_detail=f"insert_many falló tras {self._MAX_RETRIES} reintentos",
        )

    async def find_one(self, query: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            return await self._collection.find_one(query)
        except (ConnectionFailure, NetworkTimeout) as e:
            logger.error("Error de conexion en find_one", context={"collection": self._collection.name, "error": str(e)})
            self._classify_and_raise(e, operation="find_one")
        except PyMongoError as e:
            logger.error("Error en find_one", context={"collection": self._collection.name, "error": str(e)})
            self._classify_and_raise(e, operation="find_one", collection=self._collection.name)

    async def find_many(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        try:
            return await self._collection.find(query).to_list(length=None)
        except (ConnectionFailure, NetworkTimeout) as e:
            logger.error("Error de conexion en find_many", context={"collection": self._collection.name, "error": str(e)})
            self._classify_and_raise(e, operation="find_many")
        except PyMongoError as e:
            logger.error("Error en find_many", context={"collection": self._collection.name, "error": str(e)})
            self._classify_and_raise(e, operation="find_many", collection=self._collection.name)

    async def update_one(self, query: Dict[str, Any], update: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            return await self._collection.update_one(query, update)
        except (ConnectionFailure, NetworkTimeout) as e:
            logger.error("Error de conexion en update_one", context={"collection": self._collection.name, "error": str(e)})
            self._classify_and_raise(e, operation="update_one")
        except PyMongoError as e:
            logger.error("Error en update_one", context={"collection": self._collection.name, "error": str(e)})
            self._classify_and_raise(e, operation="update_one", collection=self._collection.name)

    async def update_one_upsert(self, query: Dict[str, Any], update: Dict[str, Any]):
        try:
            return await self._collection.update_one(query, update, upsert=True)
        except DuplicateKeyError as e:
            logger.error("Clave duplicada en upsert", context={"collection": self._collection.name, "error": str(e)})
            self._classify_and_raise(e, operation="update_one_upsert", collection=self._collection.name)
        except (ConnectionFailure, NetworkTimeout) as e:
            logger.error("Error de conexion en update_one_upsert", context={"collection": self._collection.name, "error": str(e)})
            self._classify_and_raise(e, operation="update_one_upsert")
        except PyMongoError as e:
            logger.error("Error en update_one_upsert", context={"collection": self._collection.name, "error": str(e)})
            self._classify_and_raise(e, operation="update_one_upsert", collection=self._collection.name)

    async def delete_one(self, query: Dict[str, Any]) -> Optional[int]:
        try:
            res: DeleteResult = await self._collection.delete_one(query)
            return res.deleted_count
        except (ConnectionFailure, NetworkTimeout) as e:
            logger.error("Error de conexion en delete_one", context={"collection": self._collection.name, "error": str(e)})
            self._classify_and_raise(e, operation="delete_one")
        except PyMongoError as e:
            logger.error("Error en delete_one", context={"collection": self._collection.name, "error": str(e)})
            self._classify_and_raise(e, operation="delete_one", collection=self._collection.name)

    def _is_rate_limit_error(self, exc: PyMongoError) -> bool:
        if isinstance(exc, BulkWriteError):
            if exc.details and "writeErrors" in exc.details:
                for write_error in exc.details["writeErrors"]:
                    error_code = write_error.get("code")
                    error_message = write_error.get("errmsg", "")
                    if error_code == 16500 or "429" in error_message or "TooManyRequests" in error_message:
                        return True
        elif isinstance(exc, PyMongoError):
            error_message = str(exc)
            if "429" in error_message or "TooManyRequests" in error_message or "Request rate os large" in error_message:
                return True
        return False

    def _extract_retry_after_ms(self, error_message: str) -> int | None:
        match = re.search(r"RetryAfterMs[=:](\d+)", error_message, re.IGNORECASE)
        if match:
            return int(match.group(1))
        return None
