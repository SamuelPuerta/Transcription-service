from .base import DomainException

class InvalidTranscriptionJobPayload(DomainException):
    def __init__(self, detail: str):
        super().__init__(f"Payload de transcripcion invalido: {detail}", "invalid_transcription_job_payload")

class UnsupportedAudioFormat(DomainException):
    def __init__(self, audio_format: str):
        super().__init__(f"Formato de audio no soportado: {audio_format}", "unsupported_audio_format")

class InvalidTranscriptionStatus(DomainException):
    def __init__(self, status: str):
        super().__init__(f"Estado de transcripcion invalido: {status}", "invalid_transcription_status")

class TranscriptionNotReady(DomainException):
    def __init__(self, transcription_id: str):
        super().__init__(f"Transcripcion aun en progreso: {transcription_id}", "transcription_not_ready")

class TranscriptionResultMissing(DomainException):
    def __init__(self, transcription_id: str):
        super().__init__(f"Resultado de transcripcion no disponible: {transcription_id}", "transcription_result_missing")

class FileAlreadyFinalized(DomainException):
    def __init__(self, file_id: str, status: str):
        super().__init__(f"Archivo ya finalizado con estado {status}: {file_id}", "file_already_finalized")

class MissingStorageRoutingData(DomainException):
    def __init__(self, detail: str):
        super().__init__(f"Datos de almacenamiento incompletos: {detail}", "missing_storage_routing_data")