from typing import Optional
import httpx
from tenacity import (
    retry,
    retry_if_not_exception_type,
    stop_after_attempt,
    wait_exponential,
)
from src.config.logger import logger
from src.domain.exceptions.base import DomainException
from src.domain.exceptions.transcription_exceptions import TranscriptionResultMissing
from src.domain.ports.infrastructure.ai_gateway.ai_gateway import AIGateway
from src.infrastructure.exceptions.ai_gateway_exceptions import (
    ExternalServiceError,
    ExternalTimeoutError,
    MappingError,
)

class AIGatewayAdapter(AIGateway):
    def __init__(self, client: Optional[httpx.AsyncClient] = None) -> None:
        self._endpoint = None
        self._client: Optional[httpx.AsyncClient] = client

    async def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None:
            try:
                from src.config.settings import settings

                self._endpoint = settings.ai_gateway_endpoint.rstrip("/")
                timeout = httpx.Timeout(connect=5.0, read=60.0, write=10.0, pool=5.0)
                self._client = httpx.AsyncClient(base_url=self._endpoint, timeout=timeout)
            except Exception as e:
                logger.error("No se pudo inicializar el cliente del AI Gateway", context={"error": str(e)})
                raise ExternalServiceError(
                    f"No se pudo inicializar el cliente del AI Gateway: {e}",
                    original_exception=e,
                ) from e
        return self._client

    @staticmethod
    def _classify_and_raise(
        exc: Exception,
        *,
        operation: str,
        transcription_id: Optional[str] = None,
    ) -> None:
        if isinstance(exc, httpx.ReadTimeout):
            raise ExternalTimeoutError(
                f"Timeout en operacion '{operation}' del AI Gateway",
                original_exception=exc,
            ) from exc
        if isinstance(exc, httpx.ConnectError):
            raise ExternalServiceError(
                f"No se pudo conectar al AI Gateway en operacion '{operation}'",
                original_exception=exc,
            ) from exc
        if isinstance(exc, httpx.HTTPStatusError):
            if exc.response.status_code == 404 and transcription_id:
                raise TranscriptionResultMissing(transcription_id)
            raise ExternalServiceError(
                f"HTTP {exc.response.status_code} en operacion '{operation}': {exc}",
                original_exception=exc,
            ) from exc
        if isinstance(exc, httpx.HTTPError):
            raise ExternalServiceError(
                f"Error HTTP en operacion '{operation}': {exc}",
                original_exception=exc,
            ) from exc
        if isinstance(exc, (KeyError, ValueError, TypeError)):
            raise MappingError(
                f"Error al parsear respuesta de '{operation}': {exc}",
                original_exception=exc,
            ) from exc
        raise ExternalServiceError(
            f"Error inesperado en operacion '{operation}': {exc}",
            original_exception=exc,
        ) from exc

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_not_exception_type(DomainException),
        reraise=True,
    )
    async def chat_completion(self, system_prompt: str, user_prompt: str) -> str:
        try:
            client = await self._ensure_client()
            response = await client.post(
                "/chat",
                json=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
            )
            response.raise_for_status()
        except (ExternalTimeoutError, ExternalServiceError):
            raise
        except Exception as e:
            logger.error("Error en chat_completion del AI Gateway", context={"error": str(e), "error_type": type(e).__name__})
            self._classify_and_raise(e, operation="chat_completion")
        try:
            result = self._extract_chat_completion_content(response.json())
            logger.info("Chat completion obtenido del AI Gateway")
            return result
        except (KeyError, ValueError, TypeError) as e:
            logger.error("Error parseando respuesta de chat_completion", context={"error": str(e)})
            self._classify_and_raise(e, operation="chat_completion")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_not_exception_type(DomainException),
        reraise=True,
    )
    async def create_transcription(self, audio_uri: str) -> str:
        try:
            client = await self._ensure_client()
            response = await client.post(f"/transcription?audio_uri={audio_uri}")
            response.raise_for_status()
        except (ExternalTimeoutError, ExternalServiceError):
            raise
        except Exception as e:
            logger.error("Error creando trabajo de transcripcion en AI Gateway", context={"error": str(e), "error_type": type(e).__name__})
            self._classify_and_raise(e, operation="create_transcription")
        try:
            transcription_id = response.json()
            logger.info("Trabajo de transcripcion creado en AI Gateway", context={"transcription_id": transcription_id})
            return transcription_id
        except (ValueError, TypeError) as e:
            logger.error("Error parseando respuesta de create_transcription", context={"error": str(e)})
            self._classify_and_raise(e, operation="create_transcription")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_not_exception_type(DomainException),
        reraise=True,
    )
    async def get_transcription_status(self, transcription_id: str) -> str:
        try:
            client = await self._ensure_client()
            response = await client.get(f"/transcription/status/{transcription_id}")
            response.raise_for_status()
        except (ExternalTimeoutError, ExternalServiceError, TranscriptionResultMissing):
            raise
        except Exception as e:
            logger.error("Error consultando estado de transcripcion en AI Gateway", context={"transcription_id": transcription_id, "error": str(e), "error_type": type(e).__name__})
            self._classify_and_raise(
                e,
                operation="get_transcription_status",
                transcription_id=transcription_id,
            )
        try:
            return response.json()
        except (ValueError, TypeError) as e:
            logger.error("Error parseando respuesta de get_transcription_status", context={"transcription_id": transcription_id, "error": str(e)})
            self._classify_and_raise(e, operation="get_transcription_status")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_not_exception_type(DomainException),
        reraise=True,
    )
    async def get_transcription_result(self, transcription_id: str) -> str:
        try:
            client = await self._ensure_client()
            response = await client.get(f"/transcription/result/{transcription_id}")
            response.raise_for_status()
        except (ExternalTimeoutError, ExternalServiceError, TranscriptionResultMissing):
            raise
        except Exception as e:
            logger.error("Error obteniendo resultado de transcripcion del AI Gateway", context={"transcription_id": transcription_id, "error": str(e), "error_type": type(e).__name__})
            self._classify_and_raise(
                e,
                operation="get_transcription_result",
                transcription_id=transcription_id,
            )
        try:
            result = response.json()
            logger.info("Resultado de transcripcion obtenido del AI Gateway", context={"transcription_id": transcription_id})
            return result
        except (ValueError, TypeError) as e:
            logger.error("Error parseando resultado de transcripcion", context={"transcription_id": transcription_id, "error": str(e)})
            self._classify_and_raise(e, operation="get_transcription_result")

    async def aclose(self) -> None:
        if self._client is not None:
            try:
                await self._client.aclose()
            finally:
                self._client = None

    def _extract_chat_completion_content(self, raw: object) -> str:
        if not isinstance(raw, dict):
            raise MappingError("chat completion payload must be an object")
        choices = raw.get("choices")
        if not isinstance(choices, list) or not choices:
            raise MappingError("choices is required")
        message = choices[0].get("message")
        if not isinstance(message, dict):
            raise MappingError("message is required")
        content = message.get("content")
        if not isinstance(content, str):
            raise MappingError("content must be a string")
        return content
