from urllib.parse import unquote
import os
from datetime import date
import json
from azure.servicebus import ServiceBusReceivedMessage
from src.application.dtos.request.ingestion import StorageEventRequestDTO
from src.domain.exceptions.ingestion_exceptions import InvalidStorageEvent

def extract_blob_event_info(message: ServiceBusReceivedMessage) -> StorageEventRequestDTO:
    raw = b"".join(bytes(chunk) for chunk in message.body)
    event = json.loads(raw.decode("utf-8"))
    subject = event.get("subject")
    if not subject:
        raise InvalidStorageEvent("CloudEvent sin subject")
    prefix = "/blobServices/default/containers/"
    if not subject.startswith(prefix):
        raise InvalidStorageEvent(f"Subject inesperado: {subject}")
    remainder = subject[len(prefix):]
    parts = remainder.split("/blobs/", 1)
    if len(parts) != 2:
        raise InvalidStorageEvent(f"No se pudo parsear container/blobs en subject: {subject}")
    container = parts[0]
    blob_path = unquote(parts[1])
    if not container or not blob_path:
        raise InvalidStorageEvent(f"Container o blob_path vacío en subject: {subject}")
    segments = blob_path.split("/")
    if len(segments) < 3:
        raise InvalidStorageEvent(f"blob_path inválido, esperado <initiative>/<Correlation ID>/<file>: {blob_path}")
    initiative_id = segments[0]
    correlation_id = segments[1]
    if not initiative_id:
        raise InvalidStorageEvent(f"initiative_id vacío en blob_path: {blob_path}")
    file_name = os.path.basename(blob_path)
    if not file_name:
        raise InvalidStorageEvent(f"file_name vacío en blob_path: {blob_path}")
    data = event.get("data")
    blob_url = data.get("url")
    return StorageEventRequestDTO(
        batch_id=f"{initiative_id}:{correlation_id}",
        initiative_id=initiative_id,
        blob_url=blob_url,
        file_name=file_name,
        container_name=container,
        correlation_id=correlation_id
    )