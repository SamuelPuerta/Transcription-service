import uuid
from kink import di
from src.application.mappers.service_bus_job_mapper import to_transcription_job_entity
from src.application.dtos.request.ingestion import StorageEventRequestDTO
from src.config.logger import logger
from src.domain.entities.call_processing_entity import CallProcessingEntity
from src.domain.entities.files_processing_entity import FilesProcessingEntity
from src.domain.ports.application.ingestion import ProcessStorageEventUseCase
from src.domain.ports.infrastructure.ai_gateway.ai_gateway import AIGateway
from src.domain.ports.infrastructure.persistence.call_processing import CallProcessing
from src.domain.ports.infrastructure.persistence.files_processing import FilesProcessing
from src.domain.ports.infrastructure.service_bus.transcription_jobs import (
    TranscriptionJobsPublisher,
)

class ProcessStorageEvent(ProcessStorageEventUseCase):
    def __init__(
        self,
        call_processing_repo: CallProcessing | None = None,
        files_processing_repo: FilesProcessing | None = None,
        transcription_jobs_publisher: TranscriptionJobsPublisher | None = None,
        ai_gateway_adapter: AIGateway | None = None,
    ) -> None:
        self._call_processing_repo = call_processing_repo or di[CallProcessing]()
        self._files_processing_repo = files_processing_repo or di[FilesProcessing]()
        self._transcription_jobs_publisher = (
            transcription_jobs_publisher or di[TranscriptionJobsPublisher]()
        )
        self._ai_gateway_adapter = ai_gateway_adapter or di[AIGateway]()

    async def execute(self, storage_event: StorageEventRequestDTO) -> None:
        log = logger.bind(correlation_id=storage_event.correlation_id)
        log.info("Evento de almacenamiento recibido", context={
            "batch_id": storage_event.batch_id,
            "initiative_id": storage_event.initiative_id,
            "file_name": storage_event.file_name,
            "blob_url": storage_event.blob_url,
            "correlation_id": storage_event.correlation_id,
        })
        call_data = CallProcessingEntity(
            batch_id=storage_event.batch_id,
            initiative_id=storage_event.initiative_id,
            storage_container=storage_event.container_name,
        )
        file_data = FilesProcessingEntity(
            file_id=str(uuid.uuid5(uuid.NAMESPACE_URL, storage_event.blob_url)),
            batch_id=storage_event.batch_id,
            file_name=storage_event.file_name,
            blob_url=storage_event.blob_url,
            xlsx_name="Copia_CSV.xlsx",
        )
        await self._call_processing_repo.create(call_data)
        file_id = await self._files_processing_repo.create(file_data)
        is_new_file = file_id is not None
        if not file_id:
            existing = await self._files_processing_repo.get_by_blob_url(storage_event.blob_url)
            file_id = existing.file_id if existing else None
            log.info("Redelivery detectado, archivo ya existe", context={
                "file_id": file_id,
                "batch_id": storage_event.batch_id,
                "blob_url": storage_event.blob_url,
                "correlation_id": storage_event.correlation_id,
            })
        if not file_id:
            return
        if is_new_file:
            await self._call_processing_repo.increment_total_files(storage_event.batch_id)
        await self._call_processing_repo.mark_as_started(storage_event.batch_id)
        transcription_id = await self._ai_gateway_adapter.create_transcription(audio_uri=storage_event.blob_url)
        await self._transcription_jobs_publisher.enqueue(
            to_transcription_job_entity(
                storage_event,
                file_id=file_id,
                transcription_id=transcription_id,
                correlation_id=storage_event.correlation_id,
            )
        )
        log.info("Trabajo de transcripcion encolado", context={
            "file_id": file_id,
            "batch_id": storage_event.batch_id,
            "transcription_id": transcription_id,
            "correlation_id": storage_event.correlation_id,
        })
