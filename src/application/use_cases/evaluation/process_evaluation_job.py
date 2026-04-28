from kink import di
from src.application.parsers.evaluation_json import parse_json_str
from src.application.validators.evaluation_job import validate_job
from src.application.dtos.request.evaluation import ProcessEvaluationJobRequestDTO
from src.application.mappers.evaluation_job_mapper import (
    to_complete_batch_request,
    to_finalize_file_evaluation_request,
)
from src.config.logger import logger
from src.domain.exceptions.evaluation_exceptions import MissingTranscriptionForEvaluation
from src.domain.exceptions.ingestion_exceptions import FileProcessingNotFound, InitiativeNotFound
from src.domain.ports.application.docs_gen import CompleteBatchUseCase
from src.domain.ports.application.evaluation import (
    FinalizeFileEvaluationUseCase,
    ProcessEvaluationJobUseCase,
)
from src.domain.ports.infrastructure.ai_gateway.ai_gateway import AIGateway
from src.domain.ports.infrastructure.persistence.call_processing import CallProcessing
from src.domain.ports.infrastructure.persistence.files_processing import FilesProcessing
from src.domain.ports.infrastructure.persistence.initiatives import Initiatives
from src.domain.value_objects.files_processing_status import FilesProcessingStatus

EVALUATION_USER_PROMPT_TEMPLATE = (
    "TRANSCRIPCION:\n"
    "{transcription}\n\n"
    "CONSECUTIVO_SISTEMA:\n"
    "{consecutive}\n\n"
    "INSTRUCCION:\n"
    "Devuelve unicamente el JSON con la estructura requerida."
)

class ProcessEvaluationJob(ProcessEvaluationJobUseCase):
    def __init__(
        self,
        ai_gateway_adapter: AIGateway | None = None,
        files_processing_repo: FilesProcessing | None = None,
        call_processing_repo: CallProcessing | None = None,
        initiative_repo: Initiatives | None = None,
        finalize_file_evaluation_use_case: FinalizeFileEvaluationUseCase | None = None,
        complete_batch_use_case: CompleteBatchUseCase | None = None,
    ) -> None:
        self._ai_gateway_adapter = ai_gateway_adapter or di[AIGateway]()
        self._files_repo = files_processing_repo or di[FilesProcessing]()
        self._call_repo = call_processing_repo or di[CallProcessing]()
        self._initiative_repo = initiative_repo or di[Initiatives]()
        self._finalize_file_evaluation = (
            finalize_file_evaluation_use_case or di[FinalizeFileEvaluationUseCase]()
        )
        self._complete_batch = complete_batch_use_case or di[CompleteBatchUseCase]()

    async def execute(self, evaluation: ProcessEvaluationJobRequestDTO) -> None:
        log = logger.bind(correlation_id=evaluation.correlation_id)
        validate_job(evaluation)
        file_entity = await self._files_repo.get_by_id(evaluation.file_id)
        if not file_entity:
            raise FileProcessingNotFound(evaluation.file_id)
        if file_entity.status in [FilesProcessingStatus.COMPLETED, FilesProcessingStatus.FAILED]:
            log.info("Job de evaluacion ignorado, archivo ya en estado terminal", context={
                "file_id": evaluation.file_id,
                "batch_id": evaluation.batch_id,
                "status": file_entity.status,
                "correlation_id": evaluation.correlation_id,
            })
            return
        log.info("Procesando job de evaluacion", context={
            "file_id": evaluation.file_id,
            "batch_id": evaluation.batch_id,
            "initiative_id": evaluation.initiative_id,
            "correlation_id": evaluation.correlation_id,
        })
        try:
            initiative_info = await self._initiative_repo.get_by_name(initiative=evaluation.initiative_id)
            if not initiative_info:
                raise InitiativeNotFound(evaluation.initiative_id)
            transcription_text = file_entity.transcription
            if not transcription_text:
                raise MissingTranscriptionForEvaluation(evaluation.file_id)
            user_prompt = EVALUATION_USER_PROMPT_TEMPLATE.format(
                transcription=transcription_text,
                consecutive=file_entity.consecutive or "",
            )
            response = await self._ai_gateway_adapter.chat_completion(
                system_prompt=initiative_info.configuration.prompt,
                user_prompt=user_prompt,
            )
            evaluation_json = parse_json_str(response)
            await self._finalize_file_evaluation.execute(
                to_finalize_file_evaluation_request(
                    evaluation,
                    evaluation_json=evaluation_json,
                )
            )
            await self._complete_batch.execute(
                to_complete_batch_request(evaluation)
            )
            log.info("Job de evaluacion completado", context={
                "file_id": evaluation.file_id,
                "batch_id": evaluation.batch_id,
                "initiative_id": evaluation.initiative_id,
                "correlation_id": evaluation.correlation_id,
            })
        except Exception as e:
            await self._files_repo.mark_as_failed(evaluation.file_id, str(e))
            await self._call_repo.increment_failed(evaluation.batch_id, 1)
            await self._call_repo.check_completion(evaluation.batch_id)
            log.exception("Job de evaluacion fallido", context={
                "file_id": evaluation.file_id,
                "batch_id": evaluation.batch_id,
                "initiative_id": evaluation.initiative_id,
                "correlation_id": evaluation.correlation_id,
                "error": str(e),
            })
            raise
