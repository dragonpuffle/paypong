import asyncio
from uuid import uuid4

import pytest
from pamqp.commands import Basic

from paypong.domain.models import OutboxEvent
from paypong.infrastructure.rabbitmq.publisher import RabbitEventPublisher


async def test_publisher_timeout_covers_wait_for_connection_readiness():
    class ReconnectingBroker:
        async def publish(self, *args, **kwargs):
            # aio-pika may wait for connection readiness before its publish timeout.
            await asyncio.sleep(0.05)
            return Basic.Ack()

    with pytest.raises(TimeoutError):
        await RabbitEventPublisher(ReconnectingBroker(), timeout=0.01).publish(
            OutboxEvent(uuid4(), None, {}),
        )
