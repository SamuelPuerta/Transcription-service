from src.application.dtos.request.docs_gen import CompleteBatchRequestDTO
from src.application.dtos.request.evaluation import ProcessEvaluationJobRequestDTO

def to_finalize_file_evaluation_request(
    evaluation: ProcessEvaluationJobRequestDTO,
    *,
    evaluation_json: dict,
) -> ProcessEvaluationJobRequestDTO:
    return ProcessEvaluationJobRequestDTO(
        file_id=evaluation.file_id,
        batch_id=evaluation.batch_id,
        initiative_id=evaluation.initiative_id,
        evaluation_json=evaluation_json,
        correlation_id=evaluation.correlation_id,
    )

def to_complete_batch_request(
    evaluation: ProcessEvaluationJobRequestDTO,
) -> CompleteBatchRequestDTO:
    return CompleteBatchRequestDTO(
        batch_id=evaluation.batch_id,
        initiative_id=evaluation.initiative_id,
        correlation_id=evaluation.correlation_id,
    )
