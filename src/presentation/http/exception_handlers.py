from fastapi import Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from src.config.logger import logger
from src.domain.exceptions.base import DomainException
from src.infrastructure.exceptions.base import InfrastructureException

def _status_for_domain(exc: DomainException) -> int:
    code = exc.code or ""
    client_errors = {
        "invalid_storage_event",
        "invalid_transcription_job_payload",
        "invalid_evaluation_job_payload",
        "invalid_evaluation_prompt",
        "invalid_evaluation_response_format",
    }
    not_found = {
        "initiative_not_found",
        "batch_not_found",
        "file_processing_not_found",
    }
    conflict = {
        "duplicate_file_in_batch",
        "invalid_file_status_transition",
        "invalid_batch_status_transition",
        "batch_totals_mismatch",
        "file_already_finalized",
    }
    failed_dep = {
        "transcription_not_ready",
        "transcription_result_missing",
        "missing_transcription_for_evaluation",
        "evaluation_data_incomplete",
        "missing_storage_routing_data",
    }
    unsupported = {"unsupported_audio_format"}
    unavailable = {"database_error", "database_unavailable"}
    if code in client_errors:
        return 400
    if code in not_found:
        return 404
    if code in conflict:
        return 409
    if code in failed_dep:
        return 424
    if code in unsupported:
        return 415
    if code in unavailable:
        return 503
    return 400

async def handle_domain_exception(request: Request, exc: DomainException):
    status_code = _status_for_domain(exc)
    body = {
        "type": exc.code,
        "title": "Domain error",
        "detail": exc.message,
    }
    if exc.extra:
        body["extra"] = exc.extra
    logger.info("Excepcion de dominio", context={
        "path": str(request.url.path),
        "code": exc.code,
        "detail": exc.message,
        "status_code": status_code,
    })
    return JSONResponse(status_code=status_code, content=body)

async def handle_validation_error(request: Request, exc: ValidationError):
    logger.warning("Error de validacion de request", context={
        "path": str(request.url.path),
        "errors": exc.errors(),
    })
    return JSONResponse(
        status_code=422,
        content={
            "type": "validation_error",
            "title": "Validation error",
            "detail": "Request validation failed",
            "errors": exc.errors(),
        },
    )

async def handle_infrastructure_exception(request: Request, exc: InfrastructureException):
    logger.error("Excepcion de infraestructura no controlada", context={
        "path": str(request.url.path),
        "error_code": exc.error_code,
        "error": str(exc),
    }, exc_info=exc)
    return JSONResponse(
        status_code=503,
        content={
            "type": exc.error_code,
            "title": "Service unavailable",
            "detail": "A dependent service is temporarily unavailable",
        },
    )

async def handle_unhandled_exception(request: Request, exc: Exception):
    logger.error("Excepcion no controlada", context={
        "path": str(request.url.path),
        "error_type": type(exc).__name__,
        "error": str(exc),
    }, exc_info=exc)
    return JSONResponse(
        status_code=500,
        content={
            "type": "internal_error",
            "title": "Internal server error",
            "detail": "An unexpected error occurred",
        },
    )
