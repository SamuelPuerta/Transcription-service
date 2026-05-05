from fastapi import Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from src.config.logger import logger
from src.domain.exceptions.base import DomainException
from src.infrastructure.exceptions.base import InfrastructureException

_DOMAIN_STATUS_MAP = {
    400: {
        "invalid_storage_event",
        "invalid_transcription_job_payload",
        "invalid_evaluation_job_payload",
        "invalid_evaluation_prompt",
        "invalid_evaluation_response_format",
    },
    404: {
        "initiative_not_found",
        "batch_not_found",
        "file_processing_not_found",
    },
    409: {
        "duplicate_file_in_batch",
        "invalid_file_status_transition",
        "invalid_batch_status_transition",
        "batch_totals_mismatch",
        "file_already_finalized",
    },
    424: {
        "transcription_not_ready",
        "transcription_result_missing",
        "missing_transcription_for_evaluation",
        "evaluation_data_incomplete",
        "missing_storage_routing_data",
    },
    415: {"unsupported_audio_format"},
    503: {"database_error", "database_unavailable"},
}


def _request_path(request: Request) -> str:
    return str(request.url.path)


def _response_body(
    *,
    error_type: str | None,
    title: str,
    detail: str,
    extra: dict | None = None,
) -> dict:
    body = {
        "type": error_type,
        "title": title,
        "detail": detail,
    }
    if extra:
        body["extra"] = extra
    return body


def _json_error_response(
    *,
    status_code: int,
    error_type: str | None,
    title: str,
    detail: str,
    extra: dict | None = None,
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content=_response_body(
            error_type=error_type,
            title=title,
            detail=detail,
            extra=extra,
        ),
    )


def _status_for_domain(exc: DomainException) -> int:
    code = exc.code or ""

    for status_code, codes in _DOMAIN_STATUS_MAP.items():
        if code in codes:
            return status_code
    return 400


async def handle_domain_exception(request: Request, exc: DomainException):
    status_code = _status_for_domain(exc)
    logger.info(
        "Excepcion de dominio",
        context={
            "path": _request_path(request),
            "code": exc.code,
            "detail": exc.message,
            "status_code": status_code,
        },
    )
    return _json_error_response(
        status_code=status_code,
        error_type=exc.code,
        title="Domain error",
        detail=exc.message,
        extra=exc.extra,
    )


async def handle_validation_error(request: Request, exc: ValidationError):
    errors = exc.errors()
    logger.warning(
        "Error de validacion de request",
        context={
            "path": _request_path(request),
            "errors": errors,
        },
    )
    return JSONResponse(
        status_code=422,
        content={
            "type": "validation_error",
            "title": "Validation error",
            "detail": "Request validation failed",
            "errors": errors,
        },
    )


async def handle_infrastructure_exception(request: Request, exc: InfrastructureException):
    logger.error(
        "Excepcion de infraestructura no controlada",
        context={
            "path": _request_path(request),
            "error_code": exc.error_code,
            "error": str(exc),
        },
        exc_info=exc,
    )
    return _json_error_response(
        status_code=503,
        error_type=exc.error_code,
        title="Service unavailable",
        detail="A dependent service is temporarily unavailable",
    )


async def handle_unhandled_exception(request: Request, exc: Exception):
    logger.error(
        "Excepcion no controlada",
        context={
            "path": _request_path(request),
            "error_type": type(exc).__name__,
            "error": str(exc),
        },
        exc_info=exc,
    )
    return _json_error_response(
        status_code=500,
        error_type="internal_error",
        title="Internal server error",
        detail="An unexpected error occurred",
    )
