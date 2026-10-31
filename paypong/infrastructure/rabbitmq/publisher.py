import asyncio

from faststream.rabbit import RabbitBroker
from pamqp.commands import Basic

from paypong.domain.models import OutboxEvent


class RabbitEventPublisher:
    def __init__(self, broker: RabbitBroker, *, timeout: float = 5) -> None:
        self.broker = broker
        self.timeout = timeout

    async def publish(self, event: OutboxEvent) -> None:
        # aio-pika's own timeout begins after waiting for connection readiness.
        # Bound that wait too, so an outbox transaction never holds locks forever.
        async with asyncio.timeout(self.timeout):
            result = await self.broker.publish(
                event.payload,
                routing_key=event.routing_key,
                persist=True,
                mandatory=True,
                timeout=self.timeout,
                message_id=str(event.event_id),
                correlation_id=str(event.payment_id) if event.payment_id else None,
            )
        if not isinstance(result, Basic.Ack):
            raise RuntimeError("Broker did not confirm publication")
