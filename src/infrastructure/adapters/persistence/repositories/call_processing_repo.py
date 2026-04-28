from datetime import datetime, timezone
from pymongo.asynchronous.collection import AsyncCollection
from src.config.logger import logger
from src.infrastructure.mappers.call_processing_mapper import from_doc, to_doc
from src.domain.entities.call_processing_entity import CallProcessingEntity
from src.domain.ports.infrastructure.persistence.call_processing import CallProcessing
from src.infrastructure.adapters.persistence.repositories.mongo_generic_repo import MongoGenericRepo
from src.domain.value_objects.call_processing_status import CallProcessingStatus

class CallProcessingRepo(MongoGenericRepo, CallProcessing):
    _collection_name = "call_processing"
    _to_doc = staticmethod(to_doc)
    _from_doc = staticmethod(from_doc)
    _indexes = [
        (
            [("batch_id", 1)],
            {
                "unique": True,
                "name": "uq_batch_id",
            },
        ),
    ]

    def __init__(self, collection: AsyncCollection):
        super().__init__(collection)

    async def get_by_id(self, id: str) -> CallProcessingEntity | None:
        document = await self.find_one({"batch_id": id})
        return CallProcessingEntity.from_dict(document) if document else None

    async def create(self, batch: CallProcessingEntity) -> None:
        doc = self._to_doc(batch)
        result = await self._collection.update_one(
            {"batch_id": batch.batch_id},
            {"$setOnInsert": doc},
            upsert=True
        )
        if result.upserted_id:
            logger.info("Registro de batch creado", context={"batch_id": batch.batch_id})

    async def increment_total_files(self, batch_id: str, delta: int = 1) -> None:
        now = datetime.now(timezone.utc)
        await self._collection.update_one(
            {"batch_id": batch_id},
            {
                "$inc": {"total_files": delta},
                "$set": {"updated_at": now, "status": CallProcessingStatus.PROCESSING},
            },
        )

    async def mark_as_started(self, batch_id: str) -> None:
        now = datetime.now(timezone.utc)
        await self._collection.update_one(
            {
                "batch_id": batch_id,
                "started_at": {"$exists": False}
            },
            {
                "$set": {
                    "started_at": now,
                    "status": CallProcessingStatus.PROCESSING,
                    "updated_at": now
                }
            }
        )
        logger.info("Batch marcado como iniciado", context={"batch_id": batch_id})

    async def increment_completed(self, batch_id: str, delta: int = 1) -> None:
        now = datetime.now(timezone.utc)
        await self._collection.update_one(
            {"batch_id": batch_id},
            {
                "$inc": {"completed_files": delta, "processed_files": delta},
                "$set": {"updated_at": now},
            },
        )

    async def increment_failed(self, batch_id: str, delta: int = 1) -> None:
        now = datetime.now(timezone.utc)
        await self._collection.update_one(
            {"batch_id": batch_id},
            {
                "$inc": {"failed_files": delta, "processed_files": delta},
                "$set": {"updated_at": now},
            },
        )

    async def check_completion(self, batch_id: str) -> str:
        now = datetime.now(timezone.utc)
        await self._collection.update_one(
            {
                "batch_id": batch_id,
                "total_files": {"$gt": 0},
                "status": {"$in": [CallProcessingStatus.PENDING, CallProcessingStatus.PROCESSING]},
                "$expr": {"$eq": ["$failed_files", "$total_files"]},
            },
            {"$set": {"status": CallProcessingStatus.FAILED, "completed_at": now, "updated_at": now}},
        )
        await self._collection.update_one(
            {
                "batch_id": batch_id,
                "total_files": {"$gt": 0},
                "status": {"$in": [CallProcessingStatus.PENDING, CallProcessingStatus.PROCESSING]},
                "$expr": {"$and": [{"$eq": ["$processed_files", "$total_files"]}, {"$lt": ["$failed_files", "$total_files"]}]},
            },
            {"$set": {"status": CallProcessingStatus.COMPLETED, "completed_at": now, "updated_at": now}},
        )
        doc = await self._collection.find_one({"batch_id": batch_id}, {"status": 1})
        status = doc["status"] if doc else CallProcessingStatus.PENDING
        if status in (CallProcessingStatus.COMPLETED, CallProcessingStatus.FAILED):
            logger.info("Batch alcanzo estado terminal", context={"batch_id": batch_id, "status": status})
        return status

    async def update_status(self, batch_id: str, status: str) -> None:
        now = datetime.now(timezone.utc)
        await self._collection.update_one(
            {"batch_id": batch_id},
            {"$set": {"status": status, "updated_at": now}},
        )
        logger.info("Estado de batch actualizado", context={"batch_id": batch_id, "status": status})

    async def update(self, batch: CallProcessingEntity) -> None:
        doc = self._to_doc(batch)
        for k in ("total_files", "processed_files", "completed_files", "failed_files"):
            doc.pop(k, None)
        doc["updated_at"] = datetime.now(timezone.utc)
        await self._collection.update_one(
            {"batch_id": batch.batch_id},
            {"$set": doc},
        )

    async def delete(self, id: str) -> None:
        await self.delete_one({"batch_id": id})
