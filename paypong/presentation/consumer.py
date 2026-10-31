import asyncio
import json
import logging
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from faststream import AckPolicy
from faststream.rabbit import RabbitBroker, RabbitMessage
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from paypong.application.contracts import PaymentCreated
from paypong.application.ports import UowFactory
from paypong.application.processing import ProcessPayment
from paypong.domain.errors import PaymentNotFound
from paypong.domain.models import OutboxEvent
from paypong.infrastructure.rabbitmq.topology import PAYMENTS

logger = logging.getLogger(__name__)


class IncomingEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_id: UUID
    payment_id: UUID
    attempt: int = Field(default=1, ge=1, le=3, strict=True)


class ConsumerHandler:
    def __init__(self, process: ProcessPayment, uow_factory: UowFactory) -> None:
        self.process = process
        self.uow_factory = uow_factory

    async def handle(self, body: Any, message: RabbitMessage) -> None:
        try:
            try:
                event = IncomingEvent.model_validate(body)
            except ValidationError:
                # Persist responsibility before ACK, even for poison messages.
                poison_id = uuid5(NAMESPACE_URL, message.raw_message.body.hex())
                async with self.uow_factory() as uow:
                    await uow.outbox.add(
                        OutboxEvent(
                            event_id=poison_id,
                            payment_id=None,
                            routing_key="payments.dlq",
                            payload={
                                "reason": "invalid_event",
                                "body_hex": message.raw_message.body[:4096].hex(),
                            },
                        )
                    )
                    await uow.commit()
            else:
                logger.info(
                    "payment received payment_id=%s event_id=%s attempt=%d",
                    event.payment_id,
                    event.event_id,
                    event.attempt,
                )
                try:
                    await self.process.execute(
                        PaymentCreated(
                            event_id=event.event_id,
                            payment_id=event.payment_id,
                            attempt=event.attempt,
                        )
                    )
                except (PaymentNotFound, ValueError) as exc:
                    async with self.uow_factory() as uow:
                        await uow.outbox.add(
                            OutboxEvent(
                                event_id=uuid5(event.event_id, "invalid-state"),
                                payment_id=None,
                                routing_key="payments.dlq",
                                payload={
                                    **event.model_dump(mode="json"),
                                    "reason": type(exc).__name__,
                                },
                            )
                        )
                        await uow.commit()
        except Exception:
            logger.exception("Consumer failed before durable completion")
            # Infrastructure failure: retain original. Business retries use SQL outbox.
            await asyncio.sleep(0.5)
            await message.nack(requeue=True)
            return
        await message.ack()


def register_consumer(broker: RabbitBroker, handler: ConsumerHandler) -> None:
    async def decode(message: RabbitMessage) -> Any:
        try:
            return json.loads(message.body)
        except (ValueError, UnicodeDecodeError):
            return None

    @broker.subscriber(PAYMENTS, ack_policy=AckPolicy.MANUAL, no_reply=True, decoder=decode)
    async def consume(body: Any, message: RabbitMessage) -> None:
        await handler.handle(body, message)
