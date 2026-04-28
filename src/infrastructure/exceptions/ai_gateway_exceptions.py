from src.infrastructure.exceptions.base import InfrastructureException

class ExternalServiceError(InfrastructureException):
    def __init__(self, message: str = "Error de servicio externo", original_exception: Exception = None):
        super().__init__(
            message=message,
            error_code="external_service_error",
            original_exception=original_exception,
        )

class ExternalTimeoutError(InfrastructureException):
    def __init__(self, message: str = "Timeout de servicio externo", original_exception: Exception = None):
        super().__init__(
            message=message,
            error_code="external_timeout_error",
            original_exception=original_exception,
        )

class MappingError(InfrastructureException):
    def __init__(self, message: str = "Error al mapear la respuesta del servicio externo", original_exception: Exception = None):
        super().__init__(
            message=message,
            error_code="mapping_error",
            original_exception=original_exception,
        )
