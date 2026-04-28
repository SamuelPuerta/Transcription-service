from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from azure.servicebus import ServiceBusMessage
from azure.servicebus.aio import ServiceBusClient
from azure.servicebus.exceptions import OperationTimeoutError, ServiceBusConnectionError
from src.config.logger import logger
from src.infrastructure.exceptions.service_bus_exceptions import ServiceBusPublishError

@dataclass(frozen=True)
class BasePublisherConfig:
    connection_string: str
    queue_name: str

class BaseServiceBusPublisher:
    def __init__(self, config: BasePublisherConfig) -> None:
        self._config = config

    async def publish(
        self,
        message: ServiceBusMessage,
        *,
        scheduled_enqueue_time_utc: Optional[datetime] = None,
    ) -> None:
        try:
            async with ServiceBusClient.from_connection_string(
                self._config.connection_string
            ) as client:
                async with client.get_queue_sender(self._config.queue_name) as sender:
                    if scheduled_enqueue_time_utc is None:
                        await sender.send_messages(message)
                    else:
                        await sender.schedule_messages(
                            message,
                            scheduled_enqueue_time_utc,
                        )
        except ServiceBusConnectionError as e:
            logger.error("Error de conexion publicando en Service Bus", context={"queue": self._config.queue_name, "error": str(e)})
            raise ServiceBusPublishError(
                f"Connection error publishing queue {self._config.queue_name}",
                original_exception=e,
            ) from e
        except OperationTimeoutError as e:
            logger.error("Timeout publicando en Service Bus", context={"queue": self._config.queue_name, "error": str(e)})
            raise ServiceBusPublishError(
                f"Timeout publishing queue {self._config.queue_name}",
                original_exception=e,
            ) from e
        except Exception as e:
            logger.error("Error inesperado publicando en Service Bus", context={"queue": self._config.queue_name, "error": str(e)})
            raise ServiceBusPublishError(
                f"Unexpected publisher error in queue {self._config.queue_name}",
                original_exception=e,
            ) from e
        logger.info("Mensaje publicado en Service Bus", context={"queue": self._config.queue_name, "message_id": getattr(message, "message_id", None)})
