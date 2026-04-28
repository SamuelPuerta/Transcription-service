from datetime import datetime, timedelta, timezone
from kink import di
from src.application.validators.transcription_job import validate_job
from src.application.mappers.service_bus_job_mapper import (
    to_retry_transcription_job_entity,
)
from src.application.mappers.transcription_job_mapper import (
    to_manifest_enrichment_request,
    to_queue_evaluation_job_request,
)
from src.application.dtos.request.transcription import (
    TranscriptionJobRequestDTO,
)
from src.config.logger import logger
from src.domain.exceptions.ingestion_exceptions import BatchNotFound, FileProcessingNotFound
from src.domain.ports.application.transcription import (
    EnrichFileFromManifestUseCase,
    ProcessTranscriptionJobUseCase,
    QueueEvaluationJobUseCase,
)
from src.domain.ports.infrastructure.ai_gateway.ai_gateway import AIGateway
from src.domain.ports.infrastructure.persistence.call_processing import CallProcessing
from src.domain.ports.infrastructure.persistence.files_processing import FilesProcessing
from src.domain.ports.infrastructure.service_bus.transcription_jobs import (
    TranscriptionJobsPublisher,
)
from src.domain.value_objects.files_processing_status import FilesProcessingStatus

class ProcessTranscriptionJob(ProcessTranscriptionJobUseCase):
    def __init__(
        self,
        files_processing_repo: FilesProcessing | None = None,
        call_processing_repo: CallProcessing | None = None,
        ai_gateway_adapter: AIGateway | None = None,
        enrich_file_from_manifest_use_case: EnrichFileFromManifestUseCase | None = None,
        queue_evaluation_job_use_case: QueueEvaluationJobUseCase | None = None,
        transcription_jobs_publisher: TranscriptionJobsPublisher | None = None,
    ) -> None:
        self._files_repo = files_processing_repo or di[FilesProcessing]()
        self._call_repo = call_processing_repo or di[CallProcessing]()
        self._ai_gateway_adapter = ai_gateway_adapter or di[AIGateway]()
        self._enrich_file_from_manifest = (
            enrich_file_from_manifest_use_case or di[EnrichFileFromManifestUseCase]()
        )
        self._queue_evaluation_job = (
            queue_evaluation_job_use_case or di[QueueEvaluationJobUseCase]()
        )
        self._transcription_jobs_publisher = (
            transcription_jobs_publisher or di[TranscriptionJobsPublisher]()
        )

    async def execute(self, transcription_job: TranscriptionJobRequestDTO) -> None:
        log = logger.bind(correlation_id=transcription_job.correlation_id)
        validate_job(transcription_job)
        file_entity = await self._files_repo.get_by_id(transcription_job.file_id)
        if not file_entity:
            raise FileProcessingNotFound(transcription_job.file_id)
        if file_entity.status in [FilesProcessingStatus.COMPLETED, FilesProcessingStatus.FAILED]:
            log.info("Job de transcripcion ignorado, archivo ya en estado terminal", context={
                "file_id": transcription_job.file_id,
                "batch_id": transcription_job.batch_id,
                "correlation_id": transcription_job.correlation_id,
                "status": file_entity.status,
            })
            return
        batch = await self._call_repo.get_by_id(transcription_job.batch_id)
        if not batch:
            raise BatchNotFound(transcription_job.batch_id)
        status = await self._ai_gateway_adapter.get_transcription_status(transcription_id=transcription_job.transcription_id)
        if status in ("NotStarted", "Running"):
            log.info("Transcripcion aun no lista, reencolando job", context={
                "file_id": transcription_job.file_id,
                "batch_id": transcription_job.batch_id,
                "transcription_id": transcription_job.transcription_id,
                "correlation_id": transcription_job.correlation_id,
                "status": status,
            })
            await self._transcription_jobs_publisher.enqueue(
                to_retry_transcription_job_entity(transcription_job, storage_container=batch.storage_container),
                scheduled_enqueue_time_utc=(datetime.now(timezone.utc) + timedelta(seconds=60))
            )
            return
        log.info("Procesando resultado de transcripcion", context={
            "file_id": transcription_job.file_id,
            "batch_id": transcription_job.batch_id,
            "transcription_id": transcription_job.transcription_id,
            "correlation_id": transcription_job.correlation_id,
        })
        await self._files_repo.mark_as_processing(transcription_job.file_id)
        try:
            transcription = await self._ai_gateway_adapter.get_transcription_result(transcription_id=transcription_job.transcription_id)
            await self._enrich_file_from_manifest.execute(
                to_manifest_enrichment_request(
                    transcription_job,
                    xlsx_name=file_entity.xlsx_name or "Copia_CSV.xlsx",
                )
            )
            await self._files_repo.set_transcription(transcription_job.file_id, transcription)
            await self._queue_evaluation_job.execute(
                to_queue_evaluation_job_request(transcription_job)
            )
            log.info("Job de transcripcion completado, evaluacion encolada", context={
                "file_id": transcription_job.file_id,
                "batch_id": transcription_job.batch_id,
                "transcription_id": transcription_job.transcription_id,
                "correlation_id": transcription_job.correlation_id,
            })
        except Exception as e:
            await self._files_repo.mark_as_failed(transcription_job.file_id, str(e))
            await self._call_repo.increment_failed(transcription_job.batch_id, 1)
            await self._call_repo.check_completion(transcription_job.batch_id)
            log.exception("Job de transcripcion fallido", context={
                "file_id": transcription_job.file_id,
                "batch_id": transcription_job.batch_id,
                "transcription_id": transcription_job.transcription_id,
                "error": str(e),
                "correlation_id": transcription_job.correlation_id,
            })
            raise
