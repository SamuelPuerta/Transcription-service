class DomainException(Exception):
    def __init__(self, message: str, code: str = "domain_error", *, extra: dict | None = None):
        self.message = message
        self.code = code
        self.extra = extra or {}
        super().__init__(message)

    def __str__(self) -> str:
        prefix = f"[{self.code}] " if self.code else ""
        return f"{prefix}{self.message}"
