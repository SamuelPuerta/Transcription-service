from dataclasses import asdict
from datetime import datetime, timezone
from typing import Optional
from pymongo.asynchronous.collection import AsyncCollection
from src.config.logger import logger
from src.domain.entities.files_evaluation_enrichment_entity import (
    FilesEvaluationEnrichmentEntity,
)
from src.domain.entities.files_processing_entity import (
    EvaluationResult,
    FilesProcessingEntity,
    OperativeEvent,
)
from src.domain.entities.files_xlsx_enrichment_entity import FilesXlsxEnrichmentEntity
from src.domain.ports.infrastructure.persistence.files_processing import FilesProcessing
from src.domain.value_objects.files_processing_status import FilesProcessingStatus
from src.infrastructure.adapters.persistence.repositories.mongo_generic_repo import (
    MongoGenericRepo,
)
from src.infrastructure.mappers.files_processing_mapper import (
    evaluation_enrichment_to_doc,
    from_doc,
    to_doc,
    xlsx_enrichment_to_doc,
)

class FilesProcessingRepo(MongoGenericRepo, FilesProcessing):
    _collection_name = "files_processing"
    _to_doc = staticmethod(to_doc)
    _from_doc = staticmethod(from_doc)
    _indexes = [
        (
            [("file_id", 1)],
            {"unique": True, "name": "uq_file_id"},
        )
    ]

    def __init__(self, collection: AsyncCollection):
        super().__init__(collection)

    async def get_by_id(self, id: str) -> FilesProcessingEntity | None:
        document = await self.find_one({"file_id": id})
        return from_doc(document)

    async def create(self, file: FilesProcessingEntity) -> Optional[str]:
        doc = self._to_doc(file)
        result = await self._collection.update_one(
            {"file_id": file.file_id},
            {"$setOnInsert": doc},
            upsert=True
        )
        if result.upserted_id:
            logger.info("Registro de archivo creado", context={"file_id": file.file_id})
            return file.file_id
        return None

    async def mark_as_processing(self, file_id: str) -> None:
        now = datetime.now(timezone.utc)
        await self._collection.update_one(
            {
                "file_id": file_id,
                "processing_started_at": {"$exists": False}
            },
            {
                "$set": {
                    "status": FilesProcessingStatus.PROCESSING,
                    "processing_started_at": now,
                    "updated_at": now
                }
            }
        )
        logger.info("Archivo marcado como en procesamiento", context={"file_id": file_id})

    async def set_transcription(self, file_id: str, transcription: str) -> None:
        now = datetime.now(timezone.utc)
        await self._collection.update_one(
            {"file_id": file_id},
            {"$set": {"transcription": transcription, "updated_at": now}},
        )
        logger.info("Transcripcion almacenada", context={"file_id": file_id})

    async def set_operative_event(self, file_id: str, operative_event: OperativeEvent) -> None:
        now = datetime.now(timezone.utc)
        payload = asdict(operative_event) if operative_event else None
        await self._collection.update_one(
            {"file_id": file_id},
            {"$set": {"operative_event": payload, "updated_at": now}},
        )

    async def set_evaluation_result(self, file_id: str, evaluation_result: EvaluationResult) -> None:
        now = datetime.now(timezone.utc)
        payload = asdict(evaluation_result) if evaluation_result else None
        await self._collection.update_one(
            {"file_id": file_id},
            {"$set": {"evaluation_result": payload, "updated_at": now}},
        )
        logger.info("Resultado de evaluacion almacenado", context={"file_id": file_id})

    async def mark_as_completed(self, file_id: str) -> None:
        now = datetime.now(timezone.utc)
        await self._collection.update_one(
            {"file_id": file_id, "status": {"$ne": FilesProcessingStatus.COMPLETED}},
            {
                "$set": {
                    "status": FilesProcessingStatus.COMPLETED,
                    "completed_at": now,
                    "updated_at": now,
                    "error_message": None,
                }
            },
        )
        logger.info("Archivo marcado como completado", context={"file_id": file_id})

    async def mark_as_failed(self, file_id: str, error_message: str | None = None) -> None:
        now = datetime.now(timezone.utc)
        await self._collection.update_one(
            {"file_id": file_id, "status": {"$ne": FilesProcessingStatus.COMPLETED}},
            {
                "$set": {
                    "status": FilesProcessingStatus.FAILED,
                    "completed_at": now,
                    "updated_at": now,
                    "error_message": error_message,
                }
            },
        )
        logger.warning("Archivo marcado como fallido", context={"file_id": file_id, "error_message": error_message})

    async def update(self, file: FilesProcessingEntity) -> None:
        doc = self._to_doc(file)
        for k in (
            "status",
            "processing_started_at",
            "completed_at",
            "transcription",
            "error_message",
            "operative_event",
            "evaluation_result",
            "updated_at",
        ):
            doc.pop(k, None)
        doc["updated_at"] = datetime.now(timezone.utc)
        await self._collection.update_one(
            {"file_id": file.file_id},
            {"$set": doc},
        )

    async def apply_xlsx_enrichment(
        self,
        file_id: str,
        enrichment: FilesXlsxEnrichmentEntity,
    ) -> None:
        doc = xlsx_enrichment_to_doc(enrichment)
        doc["updated_at"] = datetime.now(timezone.utc)
        await self._collection.update_one({"file_id": file_id}, {"$set": doc})
        logger.info("Enriquecimiento XLSX aplicado", context={"file_id": file_id})

    async def apply_evaluation_enrichment(
        self,
        file_id: str,
        enrichment: FilesEvaluationEnrichmentEntity,
    ) -> None:
        doc = evaluation_enrichment_to_doc(enrichment)
        doc["updated_at"] = datetime.now(timezone.utc)
        await self._collection.update_one({"file_id": file_id}, {"$set": doc})
        logger.info("Enriquecimiento de evaluacion aplicado", context={"file_id": file_id})

    async def delete(self, id: str) -> None:
        await self.delete_one({"file_id": id})

    async def get_by_blob_url(self, blob_url: str) -> FilesProcessingEntity | None:
        document = await self.find_one({"blob_url": blob_url})
        return from_doc(document)
