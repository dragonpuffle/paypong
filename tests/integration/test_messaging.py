import asyncio
import json
import os
from functools import partial
from uuid import uuid4

import aio_pika
import httpx
import pytest
from faststream.exceptions import IncorrectState
from sqlalchemy import select

from payment_service.api import create_app
from payment_service.application.outbox import PublishOutboxBatch
from payment_service.application.processing import ProcessPayment
from payment_service.config import Settings
from payment_service.domain.models import OutboxEvent, PaymentStatus
from payment_service.infrastructure.postgres.models import OutboxRow, PaymentRow
from payment_service.infrastructure.postgres.uow import SqlUnitOfWork
from payment_service.infrastructure.rabbitmq.publisher import RabbitEventPublisher
from payment_service.infrastructure.rabbitmq.topology import declare_topology, make_broker
from payment_service.infrastructure.webhook import HttpWebhookSender
from payment_service.presentation.consumer import ConsumerHandler, register_consumer
from tests.integration.test_postgres import database as database

pytestmark = pytest.mark.integration


@pytest.fixture
async def rabbit():
    url = os.environ.get("TEST_RABBITMQ_URL")
    if not url:
        pytest.fail("TEST_RABBITMQ_URL required; use a disposable broker/vhost")
    broker = make_broker(url)
    connection = await aio_pika.connect(url, timeout=5)
    try:
        await broker.connect()
        await declare_topology(broker)
        channel = await connection.channel()
        new = await channel.get_queue("payments.new")
        dlq = await channel.get_queue("payments.dlq")
        await new.purge()
        await dlq.purge()
        yield broker, new, dlq
    finally:
        await broker.stop()
        await connection.close()


async def test_confirm_persistence_and_mandatory_routing(rabbit):
    broker, new, _ = rabbit
    event = OutboxEvent(uuid4(), None, {"hello": "world"})
    await RabbitEventPublisher(broker).publish(event)
    message = await new.get(timeout=3)
    assert json.loads(message.body) == event.payload
    assert message.message_id == str(event.event_id)
    assert message.delivery_mode == aio_pika.DeliveryMode.PERSISTENT
    await message.ack()
    with pytest.raises(aio_pika.exceptions.DeliveryError):
        await RabbitEventPublisher(broker).publish(
            OutboxEvent(uuid4(), None, {}, routing_key=f"absent-{uuid4()}"),
        )


async def test_outbox_retained_on_unavailable_broker_then_recovers(database, rabbit):
    _, sessions = database
    broker, new, _ = rabbit
    factory = partial(SqlUnitOfWork, sessions)
    event = OutboxEvent(uuid4(), None, {"hello": "recovery"})
    async with factory() as uow:
        await uow.outbox.add(event)
        await uow.commit()
    offline = make_broker(os.environ["TEST_RABBITMQ_URL"])
    try:
        with pytest.raises(IncorrectState, match="connect"):
            await PublishOutboxBatch(factory, RabbitEventPublisher(offline)).execute()
    finally:
        await offline.stop()
    async with sessions() as session:
        assert (await session.get(OutboxRow, event.event_id)).published_at is None
    assert await PublishOutboxBatch(factory, RabbitEventPublisher(broker)).execute() == 1
    delivered = await new.get(timeout=3)
    assert delivered.message_id == str(event.event_id)
    await delivered.ack()


async def test_crash_after_publish_before_sql_mark_republishes_same_id(
    database, rabbit, monkeypatch
):
    from payment_service.infrastructure.postgres.repositories import SqlOutboxRepository

    _, sessions = database
    broker, new, _ = rabbit
    factory = partial(SqlUnitOfWork, sessions)
    event = OutboxEvent(uuid4(), None, {"crash": "window"})
    async with factory() as uow:
        await uow.outbox.add(event)
        await uow.commit()
    original = SqlOutboxRepository.mark_published

    async def crash(*args):
        raise RuntimeError("crash after broker confirm")

    monkeypatch.setattr(SqlOutboxRepository, "mark_published", crash)
    with pytest.raises(RuntimeError, match="crash"):
        await PublishOutboxBatch(factory, RabbitEventPublisher(broker)).execute()
    monkeypatch.setattr(SqlOutboxRepository, "mark_published", original)
    assert await PublishOutboxBatch(factory, RabbitEventPublisher(broker)).execute() == 1
    for _ in range(2):
        msg = await new.get(timeout=3)
        assert msg.message_id == str(event.event_id)
        await msg.ack()


class Gateway:
    calls = 0

    async def charge(self, payment):
        self.calls += 1
        return PaymentStatus.SUCCEEDED


async def wait_for(predicate):
    async with asyncio.timeout(15):
        while not await predicate():  # noqa: ASYNC110 -- polling external database state
            await asyncio.sleep(0.02)


@pytest.mark.parametrize("webhook_ok", [True, False])
async def test_http_to_real_consumer_webhook_retry_and_dlq(database, rabbit, webhook_ok):
    _, sessions = database
    broker, _, dlq = rabbit
    factory = partial(SqlUnitOfWork, sessions)
    gateway = Gateway()
    notifications = []

    async def receive(request):
        notifications.append(json.loads(request.content))
        return httpx.Response(200 if webhook_ok else 503)

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as webhook_client:
        process = ProcessPayment(
            factory, gateway, HttpWebhookSender(webhook_client), retry_base_seconds=0.02
        )
        register_consumer(broker, ConsumerHandler(process, factory))
        await broker.start()
        app = create_app(Settings(api_key="integration-key-12345"), uow_factory=factory)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app), base_url="http://test"
        ) as api:
            response = await api.post(
                "/api/v1/payments",
                headers={"X-API-Key": "integration-key-12345", "Idempotency-Key": str(uuid4())},
                json={
                    "amount": "99.12",
                    "currency": "USD",
                    "description": "order",
                    "metadata": {},
                    "webhook_url": "https://customer.example/hook",
                },
            )
        assert response.status_code == 202, response.text
        payment_id = response.json()["payment_id"]
        publish = PublishOutboxBatch(factory, RabbitEventPublisher(broker))

        async def completed():
            await publish.execute()
            async with sessions() as session:
                row = await session.scalar(select(PaymentRow).where(PaymentRow.id == uuid4_id))
                return row.webhook_delivered_at is not None if webhook_ok else row.dead_lettered

        from uuid import UUID

        uuid4_id = UUID(payment_id)
        await wait_for(completed)
        assert gateway.calls == 1
        assert len(notifications) == (1 if webhook_ok else 3)
        assert len({n["notification_id"] for n in notifications}) == 1
        async with sessions() as session:
            row = await session.get(PaymentRow, uuid4_id)
            assert row.status == "succeeded"
            assert row.attempts == (1 if webhook_ok else 3)
            initial = await session.scalar(select(OutboxRow).order_by(OutboxRow.created_at))
        # A fresh delivery after completed processing must have no external effects.
        await broker.publish(initial.payload, routing_key="payments.new", persist=True)
        if not webhook_ok:
            await publish.execute()
            msg = await dlq.get(timeout=3)
            assert json.loads(msg.body)["attempt"] == 3
            await msg.ack()
        await asyncio.sleep(0.1)
        assert gateway.calls == 1
        assert len(notifications) == (1 if webhook_ok else 3)


async def test_malformed_json_goes_to_durable_dlq(database, rabbit):
    _, sessions = database
    broker, _, dlq = rabbit
    factory = partial(SqlUnitOfWork, sessions)
    register_consumer(broker, ConsumerHandler(None, factory))
    await broker.start()
    await broker.publish(
        b"{invalid json", routing_key="payments.new", content_type="application/json"
    )

    async def completed():
        async with sessions() as session:
            return await session.scalar(select(OutboxRow.event_id)) is not None

    await wait_for(completed)
    await PublishOutboxBatch(factory, RabbitEventPublisher(broker)).execute()
    msg = await dlq.get(timeout=3)
    assert json.loads(msg.body)["reason"] == "invalid_event"
    await msg.ack()
