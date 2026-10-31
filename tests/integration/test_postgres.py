"""Real PostgreSQL contracts. Run only against a disposable *_test database."""

import asyncio
import os
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
import pytest_asyncio
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.migration import MigrationContext
from sqlalchemy import func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from payment_service.domain.models import OutboxEvent, Payment, PaymentStatus, utcnow
from payment_service.infrastructure.postgres.models import Base, OutboxRow, PaymentRow
from payment_service.infrastructure.postgres.uow import SqlUnitOfWork

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[2]


def migration(connection, direction: str = "upgrade") -> None:
    config = Config(str(ROOT / "alembic.ini"))
    config.attributes["connection"] = connection
    if direction == "upgrade":
        command.upgrade(config, "head")
    else:
        command.downgrade(config, "base")


@pytest_asyncio.fixture
async def database():
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.fail("TEST_DATABASE_URL is required: integration tests must not be skipped")
    parsed = make_url(url)
    if not parsed.database or "test" not in parsed.database.lower():
        pytest.fail("TEST_DATABASE_URL must name a disposable database containing 'test'")
    engine = create_async_engine(url, connect_args={"timeout": 5})
    try:
        async with engine.begin() as connection:
            await connection.run_sync(migration)
            await connection.execute(text("TRUNCATE outbox, payments"))
        yield engine, async_sessionmaker(engine, expire_on_commit=False)
    finally:
        await engine.dispose()


def new_payment(key: str | None = None) -> Payment:
    return Payment(
        id=uuid4(),
        amount=Decimal("1234567890123456.78"),
        currency="RUB",
        description="Оплата заказа",
        metadata={"order": "42", "nested": {"items": [1, True, None]}},
        idempotency_key=key or str(uuid4()),
        webhook_url="https://client.example/webhook",
        request_hash="a" * 64,
    )


def new_event(payment: Payment, **kwargs) -> OutboxEvent:
    return OutboxEvent(
        event_id=uuid4(),
        payment_id=payment.id,
        payload={"payment_id": str(payment.id)},
        **kwargs,
    )


async def counts(factory) -> tuple[int, int]:
    async with factory() as session:
        return (
            await session.scalar(select(func.count()).select_from(PaymentRow)),
            await session.scalar(select(func.count()).select_from(OutboxRow)),
        )


async def test_migration_roundtrip_and_schema_match(database):
    engine, _ = database
    async with engine.begin() as connection:
        drift = await connection.run_sync(
            lambda sync: compare_metadata(MigrationContext.configure(sync), Base.metadata)
        )
        assert drift == []
        await connection.run_sync(lambda sync: migration(sync, "downgrade"))
        assert await connection.scalar(text("SELECT to_regclass('payments')")) is None
        await connection.run_sync(migration)
        assert await connection.scalar(text("SELECT to_regclass('outbox')")) == "outbox"


async def test_decimal_json_and_processing_state_roundtrip(database):
    _, factory = database
    payment = new_payment()
    async with SqlUnitOfWork(factory) as uow:
        assert await uow.payments.insert_if_absent(payment)
        await uow.commit()
    async with SqlUnitOfWork(factory) as uow:
        loaded = await uow.payments.get(payment.id, for_update=True)
        assert loaded == payment
        loaded.status = PaymentStatus.SUCCEEDED
        loaded.processed_at = utcnow()
        loaded.attempts = 1
        loaded.last_error = "webhook timeout"
        loaded.next_attempt_at = utcnow() + timedelta(seconds=1)
        await uow.payments.save(loaded)
        await uow.commit()
    async with SqlUnitOfWork(factory) as uow:
        assert await uow.payments.get(payment.id) == loaded
        assert await uow.payments.find_by_key(payment.idempotency_key) == loaded
        assert await uow.payments.get(uuid4()) is None


async def test_outbox_error_rolls_back_payment(database):
    _, factory = database
    payment = new_payment()
    with pytest.raises(IntegrityError):
        async with SqlUnitOfWork(factory) as uow:
            assert await uow.payments.insert_if_absent(payment)
            await uow.outbox.add(OutboxEvent(uuid4(), uuid4(), {"bad_fk": True}))
            await uow.commit()
    assert await counts(factory) == (0, 0)


async def test_exit_without_commit_and_exception_both_rollback(database):
    _, factory = database
    async with SqlUnitOfWork(factory) as uow:
        await uow.payments.insert_if_absent(new_payment())
    with pytest.raises(RuntimeError, match="after outbox"):
        async with SqlUnitOfWork(factory) as uow:
            payment = new_payment()
            await uow.payments.insert_if_absent(payment)
            await uow.outbox.add(new_event(payment))
            raise RuntimeError("after outbox")
    assert await counts(factory) == (0, 0)


async def test_concurrent_idempotency_creates_one_payment_and_event(database):
    _, factory = database
    key = str(uuid4())

    async def create():
        payment = new_payment(key)
        async with SqlUnitOfWork(factory) as uow:
            inserted = await uow.payments.insert_if_absent(payment)
            if inserted:
                await uow.outbox.add(new_event(payment))
                result = payment.id
            else:
                # The losing INSERT must leave the transaction usable.
                result = (await uow.payments.find_by_key(key)).id
            await uow.commit()
            return result

    results = await asyncio.wait_for(asyncio.gather(*(create() for _ in range(8))), timeout=15)
    assert len(set(results)) == 1
    assert await counts(factory) == (1, 1)


async def test_outbox_due_filter_skip_locked_and_idempotent_add(database):
    _, factory = database
    payment = new_payment()
    due = [new_event(payment), new_event(payment)]
    future = new_event(payment, available_at=utcnow() + timedelta(hours=1))
    async with SqlUnitOfWork(factory) as uow:
        await uow.payments.insert_if_absent(payment)
        for event in [*due, future, due[0]]:
            await uow.outbox.add(event)
        await uow.commit()
    assert await counts(factory) == (1, 3)
    async with SqlUnitOfWork(factory) as first:
        first_batch = await first.outbox.pending_batch(1)
        assert len(first_batch) == 1
        async with SqlUnitOfWork(factory) as second:
            second_batch = await asyncio.wait_for(second.outbox.pending_batch(10), timeout=2)
            assert len(second_batch) == 1
            assert first_batch[0].event_id != second_batch[0].event_id
            assert {first_batch[0].event_id, second_batch[0].event_id} == {
                event.event_id for event in due
            }
        await first.outbox.mark_published(first_batch[0].event_id, utcnow())
        await first.commit()
    async with SqlUnitOfWork(factory) as uow:
        pending = await uow.outbox.pending_batch(10)
        assert [event.event_id for event in pending] == [second_batch[0].event_id]


async def test_outbox_publish_mark_rollback_leaves_event_available(database):
    _, factory = database
    payment = new_payment()
    event = new_event(payment)
    async with SqlUnitOfWork(factory) as uow:
        await uow.payments.insert_if_absent(payment)
        await uow.outbox.add(event)
        await uow.commit()
    async with SqlUnitOfWork(factory) as uow:
        await uow.outbox.pending_batch(1)
        await uow.outbox.mark_published(event.event_id, utcnow())
        # A crash after broker publish and before SQL commit has the same SQL effect.
    async with SqlUnitOfWork(factory) as uow:
        pending = await uow.outbox.pending_batch(1)
        assert [item.event_id for item in pending] == [event.event_id]


async def test_payment_lock_excludes_concurrent_writer_and_releases_on_commit(database):
    _, factory = database
    payment = new_payment()
    async with SqlUnitOfWork(factory) as uow:
        await uow.payments.insert_if_absent(payment)
        await uow.commit()
    async with SqlUnitOfWork(factory) as first:
        locked = await first.payments.get(payment.id, for_update=True)
        async with SqlUnitOfWork(factory) as second:
            await second.session.execute(text("SET LOCAL lock_timeout = '100ms'"))
            with pytest.raises(DBAPIError, match="lock timeout"):
                await second.payments.get(payment.id, for_update=True)
        locked.attempts = 1
        await first.payments.save(locked)
        await first.commit()
    async with SqlUnitOfWork(factory) as third:
        assert (await third.payments.get(payment.id, for_update=True)).attempts == 1


async def test_malformed_message_can_have_durable_outbox_without_payment(database):
    _, factory = database
    event = OutboxEvent(
        event_id=uuid4(),
        payment_id=None,
        payload={"reason": "invalid schema", "raw": {"bad": True}},
        routing_key="payments.dlq",
    )
    async with SqlUnitOfWork(factory) as uow:
        await uow.outbox.add(event)
        await uow.outbox.add(event)
        await uow.commit()
    async with SqlUnitOfWork(factory) as uow:
        assert await uow.outbox.pending_batch(1) == [event]
    assert await counts(factory) == (0, 1)


@pytest.mark.parametrize(
    "column,value",
    [
        ("amount", "0"),
        ("currency", "'GBP'"),
        ("attempts", "4"),
        ("status", "'succeeded'"),
        ("metadata", "'[]'::jsonb"),
    ],
)
async def test_database_enforces_payment_invariants(database, column, value):
    _, factory = database
    payment = new_payment()
    async with SqlUnitOfWork(factory) as uow:
        await uow.payments.insert_if_absent(payment)
        await uow.commit()
    # The SQL fragments are a closed list of test constants, never user input.
    async with factory() as session:
        with pytest.raises(IntegrityError):
            await session.execute(text(f"UPDATE payments SET {column} = {value}"))
        await session.rollback()
