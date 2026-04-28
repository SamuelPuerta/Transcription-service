import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional
from azure.servicebus.aio import ServiceBusClient, AutoLockRenewer
from azure.servicebus import ServiceBusReceivedMessage
from azure.servicebus.exceptions import (
    MessageLockLostError,
    SessionLockLostError,
    MessageAlreadySettled,
    ServiceBusConnectionError,
    OperationTimeoutError,
)
from src.config.logger import logger
from src.infrastructure.exceptions.service_bus_exceptions import (
    ServiceBusConsumeError,
    ServiceBusMessageLockError,
)
from src.presentation.service_bus.error_policy import handle_message_error

@dataclass(frozen=True)
class BaseConsumerConfig:
    connection_string: str
    queue_name: str
    max_wait_time: int = 10
    max_message_count: int = 1
    max_lock_renewal_seconds: int = 60 * 60

class BaseServiceBusConsumer(ABC):
    def __init__(self, config: BaseConsumerConfig) -> None:
        self._config = config
        self._client: Optional[ServiceBusClient] = None
        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self._renewer = AutoLockRenewer()

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(
            self._run(),
            name=f"consumer-{self._config.queue_name}",
        )
        logger.info("Consumer iniciado", context={"consumer": self.__class__.__name__, "queue": self._config.queue_name})

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        await self._renewer.close()
        logger.info("Consumer detenido", context={"consumer": self.__class__.__name__, "queue": self._config.queue_name})

    async def _run(self) -> None:
        try:
            async with ServiceBusClient.from_connection_string(
                self._config.connection_string
            ) as client:
                self._client = client
                async with client.get_queue_receiver(
                    self._config.queue_name
                ) as receiver:
                    while not self._stop_event.is_set():
                        messages = await receiver.receive_messages(
                            max_wait_time=self._config.max_wait_time,
                            max_message_count=self._config.max_message_count,
                        )
                        if not messages:
                            continue
                        for msg in messages:
                            await self._handle_message(receiver, msg)
        except asyncio.CancelledError:
            raise
        except ServiceBusConnectionError as e:
            logger.error("Error de conexion en consumer de Service Bus", context={"queue": self._config.queue_name, "error": str(e)})
            raise ServiceBusConsumeError(
                f"Connection error consuming queue {self._config.queue_name}",
                original_exception=e,
            ) from e
        except OperationTimeoutError as e:
            logger.error("Timeout en consumer de Service Bus", context={"queue": self._config.queue_name, "error": str(e)})
            raise ServiceBusConsumeError(
                f"Timeout consuming queue {self._config.queue_name}",
                original_exception=e,
            ) from e
        except Exception as e:
            logger.error("Error inesperado en consumer de Service Bus", context={"queue": self._config.queue_name, "error_type": type(e).__name__, "error": str(e)})
            raise ServiceBusConsumeError(
                f"Unexpected consumer error in queue {self._config.queue_name}",
                original_exception=e,
            ) from e
        finally:
            self._client = None

    async def _handle_message(self, receiver, msg: ServiceBusReceivedMessage) -> None:
        msg_id = getattr(msg, "message_id", None)
        try:
            self._renewer.register(
                receiver,
                msg,
                max_lock_renewal_duration=self._config.max_lock_renewal_seconds,
            )
            payload = await self.parse_message(msg)
            await self.process(payload)
            await receiver.complete_message(msg)
        except (MessageLockLostError, SessionLockLostError) as e:
            raise ServiceBusMessageLockError(
                f"Lock lost for message {msg_id}",
                original_exception=e,
            ) from e
        except MessageAlreadySettled:
            logger.warning("Mensaje ya liquidado previamente", context={"msg_id": msg_id, "queue": self._config.queue_name})
        except Exception as exc:
            await handle_message_error(receiver, msg, exc)

    @abstractmethod
    async def parse_message(self, msg: ServiceBusReceivedMessage):
        raise NotImplementedError

    @abstractmethod
    async def process(self, payload) -> None:
        raise NotImplementedError
