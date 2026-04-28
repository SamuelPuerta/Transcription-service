from src.config.logger import logger
from src.domain.exceptions.base import DomainException
from src.infrastructure.exceptions.base import InfrastructureException
from src.infrastructure.exceptions.service_bus_exceptions import (
    ServiceBusMessageLockError,
)

def _is_dead_letter_domain(exc: DomainException) -> bool:
    code = exc.code or ""
    invalid_payload = {
        "invalid_storage_event",
        "invalid_transcription_job_payload",
        "invalid_evaluation_job_payload",
        "invalid_evaluation_prompt",
        "invalid_evaluation_response_format",
    }
    permanent_business = {
        "initiative_not_found",
        "unsupported_audio_format",
    }
    return code in invalid_payload or code in permanent_business

async def handle_message_error(receiver, message, exc: Exception):
    msg_id = getattr(message, "message_id", None)

    if isinstance(exc, DomainException):
        if _is_dead_letter_domain(exc):
            logger.warning("Mensaje enviado a DLQ por excepcion de dominio permanente", context={
                "msg_id": msg_id,
                "code": exc.code,
                "detail": exc.message,
            })
            await receiver.dead_letter_message(
                message,
                reason=exc.code,
                error_description=exc.message,
            )
            return
        logger.warning("Mensaje abandonado por excepcion de dominio recuperable", context={
            "msg_id": msg_id,
            "code": exc.code,
            "detail": exc.message,
        })
        await receiver.abandon_message(message)
        return

    if isinstance(exc, ServiceBusMessageLockError):
        logger.warning("Lock perdido en mensaje de Service Bus", context={"msg_id": msg_id})
        return

    if isinstance(exc, InfrastructureException):
        logger.error("Mensaje abandonado por excepcion de infraestructura", context={
            "msg_id": msg_id,
            "error_code": exc.error_code,
            "error": str(exc),
        })
        await receiver.abandon_message(message)
        return

    logger.exception("Mensaje abandonado por excepcion no controlada", context={
        "msg_id": msg_id,
        "error_type": type(exc).__name__,
        "error": str(exc),
    }, exc_info=exc)
    await receiver.abandon_message(message)
