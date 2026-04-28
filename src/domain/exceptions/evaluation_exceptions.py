from .base import DomainException

class InvalidEvaluationJobPayload(DomainException):
    def __init__(self, detail: str):
        super().__init__(f"Payload de evaluacion invalido: {detail}", "invalid_evaluation_job_payload")

class MissingTranscriptionForEvaluation(DomainException):
    def __init__(self, file_id: str):
        super().__init__(f"No hay transcripcion para evaluar en file_id={file_id}", "missing_transcription_for_evaluation")

class InvalidEvaluationPrompt(DomainException):
    def __init__(self):
        super().__init__("Prompt de evaluacion vacio o invalido", "invalid_evaluation_prompt")

class InvalidEvaluationResponseFormat(DomainException):
    def __init__(self, detail: str):
        super().__init__(f"Respuesta del LLM no es JSON valido: {detail}", "invalid_evaluation_response_format")

class EvaluationDataIncomplete(DomainException):
    def __init__(self, detail: str):
        super().__init__(f"Datos de evaluacion incompletos: {detail}", "evaluation_data_incomplete")