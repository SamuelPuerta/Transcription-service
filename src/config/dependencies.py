import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any, Callable, Optional
from fastapi import FastAPI
from kink import di
from motor.motor_asyncio import AsyncIOMotorClient
from src.application.use_cases.docs_gen.complete_batch import CompleteBatch
from src.application.use_cases.evaluation.finalize_file_evaluation import (
    FinalizeFileEvaluation,
)
from src.application.use_cases.evaluation.process_evaluation_job import (
    ProcessEvaluationJob,
)
from src.application.use_cases.ingestion.process_storage_event import (
    ProcessStorageEvent,
)
from src.application.use_cases.transcription.enrich_file_from_manifest import (
    EnrichFileFromManifest,
)
from src.application.use_cases.transcription.process_transcription_job import (
    ProcessTranscriptionJob,
)
from src.application.use_cases.transcription.queue_evaluation_job import (
    QueueEvaluationJob,
)
from src.config.logger import setup_logging
from src.config.settings import settings
from src.domain.ports.application.docs_gen import CompleteBatchUseCase
from src.domain.ports.application.evaluation import (
    FinalizeFileEvaluationUseCase,
    ProcessEvaluationJobUseCase,
)
from src.domain.ports.application.ingestion import ProcessStorageEventUseCase
from src.domain.ports.application.transcription import (
    EnrichFileFromManifestUseCase,
    ProcessTranscriptionJobUseCase,
    QueueEvaluationJobUseCase,
)
from src.domain.ports.infrastructure.ai_gateway.ai_gateway import AIGateway
from src.domain.ports.infrastructure.blob_storage.blob_storage import BlobStorage
from src.domain.ports.infrastructure.persistence.call_processing import CallProcessing
from src.domain.ports.infrastructure.persistence.files_processing import FilesProcessing
from src.domain.ports.infrastructure.persistence.initiatives import Initiatives
from src.domain.ports.infrastructure.service_bus.document_generation_jobs import (
    DocumentGenerationJobsPublisher,
)
from src.domain.ports.infrastructure.service_bus.evaluation_jobs import (
    EvaluationJobsPublisher,
)
from src.domain.ports.infrastructure.service_bus.transcription_jobs import (
    TranscriptionJobsPublisher,
)
from src.infrastructure.adapters.ai_gateway.ai_gateway_adapter import AIGatewayAdapter
from src.infrastructure.adapters.blob_storage.blob_storage_adapter import (
    BlobStorageAdapter,
)
from src.infrastructure.adapters.persistence.connection import (
    create_mongo_client,
    get_database,
)
from src.infrastructure.adapters.persistence.mongo_schema import (
    ensure_call_processing_collection,
    ensure_files_processing_collection,
)
from src.infrastructure.adapters.persistence.repositories.call_processing_repo import (
    CallProcessingRepo,
)
from src.infrastructure.adapters.persistence.repositories.files_processing_repo import (
    FilesProcessingRepo,
)
from src.infrastructure.adapters.persistence.repositories.initiatives_repo import (
    InitiativesRepo,
)
from src.infrastructure.adapters.service_bus.publishers.document_generation_jobs_adapter import (
    DocumentGenerationJobsPublisherAdapter,
)
from src.infrastructure.adapters.service_bus.publishers.evaluation_jobs_adapter import (
    EvaluationJobsPublisherAdapter,
)
from src.infrastructure.adapters.service_bus.publishers.transcription_jobs_adapter import (
    TranscriptionJobsPublisherAdapter,
)
from src.presentation.service_bus.consumers.evaluation_jobs_adapter import (
    EvaluationJobsConsumerAdapter,
)
from src.presentation.service_bus.consumers.storage_events_adapter import (
    StorageEventsConsumerAdapter,
)
from src.presentation.service_bus.consumers.transcription_jobs_adapter import (
    TranscriptionJobsConsumerAdapter,
)

setup_logging()
logger = logging.getLogger(__name__)

def _lazy(factory: Callable[[], Any]) -> Callable[[], Any]:
    inst = None
    def provider():
        nonlocal inst
        if inst is None:
            inst = factory()
        return inst
    return provider

def build_infra_lifespan():
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        client: Optional[AsyncIOMotorClient] = None
        client = await create_mongo_client()
        catalogo_db = get_database(client, settings.catalogo_database_name)
        protocolo_db = get_database(client, settings.protocolo_database_name)
        await asyncio.gather(
            ensure_call_processing_collection(protocolo_db),
            ensure_files_processing_collection(protocolo_db),
        )
        initiatives_collection = catalogo_db.get_collection(settings.initiative_collection)
        files_processing_collection = protocolo_db.get_collection(
            settings.files_processing_collection
        )
        call_processing_collection = protocolo_db.get_collection(
            settings.call_processing_collection
        )
        initiatives_repo = InitiativesRepo(initiatives_collection)
        call_processing_repo = CallProcessingRepo(call_processing_collection)
        files_processing_repo = FilesProcessingRepo(files_processing_collection)

        di[Initiatives] = _lazy(lambda: initiatives_repo)
        await initiatives_repo.create_index()
        di[CallProcessing] = _lazy(lambda: call_processing_repo)
        await call_processing_repo.create_index()
        di[FilesProcessing] = _lazy(lambda: files_processing_repo)
        await files_processing_repo.create_index()
        di[BlobStorage] = _lazy(lambda: BlobStorageAdapter())
        di[TranscriptionJobsPublisher] = _lazy(
            lambda: TranscriptionJobsPublisherAdapter()
        )
        di[EvaluationJobsPublisher] = _lazy(lambda: EvaluationJobsPublisherAdapter())
        di[DocumentGenerationJobsPublisher] = _lazy(
            lambda: DocumentGenerationJobsPublisherAdapter()
        )
        di[AIGateway] = _lazy(lambda: AIGatewayAdapter())
        di[ProcessStorageEventUseCase] = _lazy(lambda: ProcessStorageEvent())
        di[EnrichFileFromManifestUseCase] = _lazy(lambda: EnrichFileFromManifest())
        di[QueueEvaluationJobUseCase] = _lazy(lambda: QueueEvaluationJob())
        di[ProcessTranscriptionJobUseCase] = _lazy(lambda: ProcessTranscriptionJob())
        di[CompleteBatchUseCase] = _lazy(lambda: CompleteBatch())
        di[FinalizeFileEvaluationUseCase] = _lazy(lambda: FinalizeFileEvaluation())
        di[ProcessEvaluationJobUseCase] = _lazy(lambda: ProcessEvaluationJob())

        storage_events_consumer = StorageEventsConsumerAdapter()
        transcription_jobs_consumer = TranscriptionJobsConsumerAdapter()
        evalaution_jobs_consumer = EvaluationJobsConsumerAdapter()
        await storage_events_consumer.start()
        await transcription_jobs_consumer.start()
        await evalaution_jobs_consumer.start()

        logger.info("Transcription Service starting up")
        try:
            yield
        finally:
            await storage_events_consumer.stop()
            await transcription_jobs_consumer.stop()
            await evalaution_jobs_consumer.stop()
            try:
                await shutdown_di()
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.exception("Error during DI shutdown: %s", e)
            logger.info("Transcription Service stopped")
    return lifespan

async def shutdown_di() -> None:
    for key in (
        Initiatives,
        CallProcessing,
        FilesProcessing,
        BlobStorage,
        TranscriptionJobsPublisher,
        DocumentGenerationJobsPublisher,
    ):
        try:
            inst = di[key]()
            if hasattr(inst, "aclose"):
                res = inst.aclose()
                if hasattr(res, "__await__"):
                    await res
        except Exception:
            pass
    di.clear_cache()
