from src.infrastructure.exceptions.base import InfrastructureException

class ServiceBusPublishError(InfrastructureException):
    def __init__(self, error_detail: str, original_exception: Exception = None):
        super().__init__(
            message=f"Error al publicar mensaje en Service Bus: {error_detail}",
            error_code="service_bus_publish_error",
            original_exception=original_exception,
        )

class ServiceBusConsumeError(InfrastructureException):
    def __init__(self, error_detail: str, original_exception: Exception = None):
        super().__init__(
            message=f"Error al consumir mensaje de Service Bus: {error_detail}",
            error_code="service_bus_consume_error",
            original_exception=original_exception,
        )

class ServiceBusMessageLockError(InfrastructureException):
    def __init__(self, error_detail: str, original_exception: Exception = None):
        super().__init__(
            message=f"Error de bloqueo de mensaje en Service Bus: {error_detail}",
            error_code="service_bus_message_lock_error",
            original_exception=original_exception,
        )