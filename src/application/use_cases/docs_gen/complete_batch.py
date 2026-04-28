from kink import di
from src.config.logger import logger
from src.application.mappers.service_bus_job_mapper import to_document_generation_job_entity
from src.domain.ports.infrastructure.service_bus.document_generation_jobs import DocumentGenerationJobsPublisher
from src.application.dtos.request.docs_gen import CompleteBatchRequestDTO
from src.domain.ports.application.docs_gen import CompleteBatchUseCase
from src.domain.ports.infrastructure.persistence.call_processing import CallProcessing
from src.domain.value_objects.call_processing_status import CallProcessingStatus

class CompleteBatch(CompleteBatchUseCase):
    def __init__(
        self,
        call_processing_repo: CallProcessing | None = None,
        document_generation_jobs_publisher: DocumentGenerationJobsPublisher | None = None
    ) -> None:
        self._call_repo = call_processing_repo or di[CallProcessing]()
        self._document_generation_jobs_publisher = (
            document_generation_jobs_publisher or di[DocumentGenerationJobsPublisher]()
        )

    async def execute(self, batch: CompleteBatchRequestDTO) -> None:
        log = logger.bind(correlation_id=batch.correlation_id)
        status = await self._call_repo.check_completion(batch.batch_id)
        if status == CallProcessingStatus.COMPLETED:
            await self._document_generation_jobs_publisher.enqueue(to_document_generation_job_entity(batch))
            log.info("Trabajo de generacion de documentos encolado", context={
                "batch_id": batch.batch_id,
                "initiative_id": batch.initiative_id,
                "correlation_id": batch.correlation_id,
            })
