from kink import di
from src.config.logger import logger
from src.domain.exceptions.evaluation_exceptions import EvaluationDataIncomplete
from src.domain.exceptions.ingestion_exceptions import FileProcessingNotFound, InitiativeNotFound
from src.domain.ports.application.evaluation import FinalizeFileEvaluationUseCase
from src.application.dtos.request.evaluation import ProcessEvaluationJobRequestDTO
from src.application.mappers.evaluation_job_mapper import to_complete_batch_request
from src.application.mappers.files_processing_enrichment_mapper import (
    to_files_evaluation_enrichment_entity,
)
from src.domain.ports.application.docs_gen import CompleteBatchUseCase
from src.domain.ports.infrastructure.blob_storage.blob_storage import BlobStorage
from src.domain.ports.infrastructure.persistence.call_processing import CallProcessing
from src.domain.ports.infrastructure.persistence.files_processing import FilesProcessing
from src.domain.ports.infrastructure.persistence.initiatives import Initiatives

class FinalizeFileEvaluation(FinalizeFileEvaluationUseCase):
    def __init__(
        self,
        files_processing_repo: FilesProcessing | None = None,
        call_processing_repo: CallProcessing | None = None,
        initiative_repo: Initiatives | None = None,
        blob_storage_adapter: BlobStorage | None = None,
        complete_batch_use_case: CompleteBatchUseCase | None = None,
    ) -> None:
        self._files_repo = files_processing_repo or di[FilesProcessing]()
        self._call_repo = call_processing_repo or di[CallProcessing]()
        self._initiative_repo = initiative_repo or di[Initiatives]()
        self._blob_storage_adapter = blob_storage_adapter or di[BlobStorage]()
        self._complete_batch_use_case = complete_batch_use_case or di[CompleteBatchUseCase]()

    async def execute(self, evaluation: ProcessEvaluationJobRequestDTO) -> None:
        log = logger.bind(correlation_id=evaluation.correlation_id)
        if evaluation.evaluation_json is None:
            raise EvaluationDataIncomplete("evaluation_json es requerido")
        file_entity = await self._files_repo.get_by_id(evaluation.file_id)
        if not file_entity:
            raise FileProcessingNotFound(evaluation.file_id)
        initiative_info = await self._initiative_repo.get_by_name(initiative=evaluation.initiative_id)
        if not initiative_info:
            raise InitiativeNotFound(evaluation.initiative_id)
        existing_metadata = getattr(
            getattr(file_entity, "evaluation_result", None),
            "metadata",
            None,
        )
        await self._files_repo.apply_evaluation_enrichment(
            evaluation.file_id,
            to_files_evaluation_enrichment_entity(
                evaluation.evaluation_json,
                existing_metadata=existing_metadata,
            ),
        )
        await self._files_repo.mark_as_completed(evaluation.file_id)
        await self._call_repo.increment_completed(evaluation.batch_id, 1)
        log.info("Evaluacion de archivo finalizada", context={
            "file_id": evaluation.file_id,
            "batch_id": evaluation.batch_id,
            "initiative_id": evaluation.initiative_id,
            "correlation_id": evaluation.correlation_id,
        })
        await self._complete_batch_use_case.execute(
            to_complete_batch_request(evaluation)
        )
