import json
from typing import Any
from azure.servicebus import ServiceBusReceivedMessage
from src.application.dtos.request.transcription import TranscriptionJobRequestDTO
from src.domain.exceptions.transcription_exceptions import InvalidTranscriptionJobPayload

_REQUIRED_FIELDS = (
    "batch_id",
    "blob_url",
    "file_name",
    "file_id",
    "initiative_id",
    "transcription_id",
    "correlation_id",
)

def extract_transcription_job(message: ServiceBusReceivedMessage) -> TranscriptionJobRequestDTO:
    raw = b"".join(bytes(chunk) for chunk in message.body)
    try:
        payload: Any = json.loads(raw.decode("utf-8"))
    except Exception as e:
        raise InvalidTranscriptionJobPayload("Mensaje de job no es JSON válido") from e
    if not isinstance(payload, dict):
        raise InvalidTranscriptionJobPayload("Mensaje de job inválido: esperado objeto JSON (dict)")
    missing = [k for k in _REQUIRED_FIELDS if not payload.get(k)]
    if missing:
        raise InvalidTranscriptionJobPayload(f"Mensaje de job inválido: faltan campos requeridos: {', '.join(missing)}")
    for k in _REQUIRED_FIELDS:
        if not isinstance(payload[k], str):
            raise InvalidTranscriptionJobPayload(f"Mensaje de job inválido: '{k}' debe ser string")
    return TranscriptionJobRequestDTO(
        file_id=payload["file_id"],
        batch_id=payload["batch_id"],
        initiative_id=payload["initiative_id"],
        transcription_id=payload["transcription_id"],
        blob_url=payload["blob_url"],
        file_name=payload["file_name"],
        correlation_id=payload["correlation_id"]
    )
