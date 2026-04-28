class InfrastructureException(Exception):
    def __init__(self, message: str, error_code: str = None, original_exception: Exception = None):
        self.message = message
        self.error_code = error_code
        self.original_exception = original_exception
        super().__init__(self.message)

    def __str__(self) -> str:
        prefix = f"[{self.error_code}] " if self.error_code else ""
        return f"{prefix}{self.message}"
