from .base import DomainException

class InvalidStorageEvent(DomainException):
    def __init__(self, detail: str):
        super().__init__(f"Evento de storage invalido: {detail}", "invalid_storage_event")

class InitiativeNotFound(DomainException):
    def __init__(self, initiative_id: str):
        super().__init__(f"Iniciativa no encontrada: {initiative_id}", "initiative_not_found")

class BatchNotFound(DomainException):
    def __init__(self, batch_id: str):
        super().__init__(f"Lote no encontrado: {batch_id}", "batch_not_found")

class FileProcessingNotFound(DomainException):
    def __init__(self, file_id: str):
        super().__init__(f"Archivo no encontrado para procesamiento: {file_id}", "file_processing_not_found")

class DuplicateFileInBatch(DomainException):
    def __init__(self, blob_url: str):
        super().__init__(f"Archivo duplicado para el mismo batch: {blob_url}", "duplicate_file_in_batch")
