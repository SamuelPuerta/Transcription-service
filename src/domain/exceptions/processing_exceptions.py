from .base import DomainException

class InvalidFileStatusTransition(DomainException):
    def __init__(self, from_status: str, to_status: str):
        super().__init__(f"Transicion de estado de archivo no permitida: {from_status} -> {to_status}", "invalid_file_status_transition")

class InvalidBatchStatusTransition(DomainException):
    def __init__(self, from_status: str, to_status: str):
        super().__init__(f"Transicion de estado de lote no permitida: {from_status} -> {to_status}", "invalid_batch_status_transition")

class BatchTotalsMismatch(DomainException):
    def __init__(self, total: int, processed: int, completed: int, failed: int):
        super().__init__(f"Totales del batch inconsistentes total={total} processed={processed} completed={completed} failed={failed}", "batch_totals_mismatch")
