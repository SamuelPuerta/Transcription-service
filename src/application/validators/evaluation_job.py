from src.application.dtos.request.evaluation import ProcessEvaluationJobRequestDTO
from src.domain.exceptions.evaluation_exceptions import InvalidEvaluationJobPayload

def validate_job(evaluation: ProcessEvaluationJobRequestDTO) -> None:
        if not evaluation.file_id:
            raise InvalidEvaluationJobPayload("job.file_id es requerido")
        if not evaluation.batch_id:
            raise InvalidEvaluationJobPayload("job.batch_id es requerido")
        if not evaluation.initiative_id:
            raise InvalidEvaluationJobPayload("job.initiative_id es requerido")
        if not evaluation.correlation_id:
            raise InvalidEvaluationJobPayload("job.correlation_id es requerido")