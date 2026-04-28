from src.infrastructure.exceptions.base import InfrastructureException

class DatabaseConnectionError(InfrastructureException):
    def __init__(self, error_detail: str, original_exception: Exception = None):
        super().__init__(
            message=f"Error de conexión a la base de datos: {error_detail}",
            error_code="database_connection_error",
            original_exception=original_exception,
        )

class DatabaseOperationError(InfrastructureException):
    def __init__(self, operation: str, error_detail: str, original_exception: Exception = None):
        super().__init__(
            message=f"Error en operación de base de datos '{operation}': {error_detail}",
            error_code="database_operation_error",
            original_exception=original_exception,
        )

class DatabaseDuplicateKeyError(InfrastructureException):
    def __init__(self, collection: str, key: str, original_exception: Exception = None):
        super().__init__(
            message=f"Clave duplicada en la colección '{collection}': {key}",
            error_code="database_duplicate_key_error",
            original_exception=original_exception,
        )