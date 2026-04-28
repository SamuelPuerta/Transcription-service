import json
from typing import Any
from azure.servicebus import ServiceBusReceivedMessage
from src.application.dtos.request.evaluation import ProcessEvaluationJobRequestDTO
from src.domain.exceptions.evaluation_exceptions import InvalidEvaluationJobPayload

_REQUIRED_FIELDS = (
    "batch_id",
    "file_id",
    "initiative_id",
    "correlation_id"
)

def extract_evaluation_job(message: ServiceBusReceivedMessage) -> ProcessEvaluationJobRequestDTO:
    raw = b"".join(bytes(chunk) for chunk in message.body)
    try:
        payload: Any = json.loads(raw.decode("utf-8"))
    except Exception as e:
        raise InvalidEvaluationJobPayload("Mensaje de evaluation job no es JSON válido") from e
    if not isinstance(payload, dict):
        raise InvalidEvaluationJobPayload("Mensaje de evaluation job inválido: esperado objeto JSON (dict)")
    missing = [k for k in _REQUIRED_FIELDS if not payload.get(k)]
    if missing:
        raise InvalidEvaluationJobPayload(f"Mensaje de evaluation job inválido: faltan campos requeridos: {', '.join(missing)}")
    for k in _REQUIRED_FIELDS:
        if not isinstance(payload[k], str):
            raise InvalidEvaluationJobPayload(f"Mensaje de evaluation job inválido: '{k}' debe ser string")
    return ProcessEvaluationJobRequestDTO(
        batch_id=payload["batch_id"],
        file_id=payload["file_id"],
        initiative_id=payload["initiative_id"],
        correlation_id=payload["correlation_id"],
    )
