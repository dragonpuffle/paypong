import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest import IsolatedAsyncioTestCase
from uuid import uuid4

from paypong.application.contracts import PaymentCreated
from paypong.application.dto import CreatePaymentCommand
from paypong.application.outbox import PublishOutboxBatch
from paypong.application.payments import CreatePayment, GetPayment
from paypong.application.processing import ProcessPayment
from paypong.domain.errors import IdempotencyConflict, PaymentNotFound
from paypong.domain.models import PaymentStatus
from tests.fakes import MemoryStore, MemoryUow


def command(**changes):
    values = dict(
        amount=Decimal("12.30"),
        currency="RUB",
        description="Order",
        metadata={"order": 1},
        idempotency_key="request-1",
        webhook_url="https://example.com/hook",
    )
    values.update(changes)
    return CreatePaymentCommand(**values)


class CreatePaymentTests(IsolatedAsyncioTestCase):
    async def test_creates_pending_payment_with_atomic_outbox(self):
        store = MemoryStore()
        payment = await CreatePayment(store.uow).execute(command())
        self.assertIn(payment.id, store.payments)
        self.assertEqual(payment.status, "pending")
        self.assertEqual(len(store.events), 1)
        event = next(iter(store.events.values()))
        self.assertEqual(event.payment_id, payment.id)

    async def test_outbox_failure_rolls_back_payment(self):
        store = MemoryStore(outbox_error=True)
        with self.assertRaisesRegex(RuntimeError, "outbox unavailable"):
            await CreatePayment(store.uow).execute(command())
        self.assertEqual(store.payments, {})
        self.assertEqual(store.events, {})

    async def test_duplicate_normalized_request_creates_one_event(self):
        store = MemoryStore()
        create = CreatePayment(store.uow)
        first = await create.execute(
            command(
                amount=Decimal("12.3"),
                metadata={
                    "z": [1, 0],
                    "a": {"x": 2, "y": "text"},
                },
            )
        )
        second = await create.execute(
            command(
                amount=Decimal("12.30"),
                metadata={
                    "a": {"y": "text", "x": 2.0},
                    "z": [1.0, -0.0],
                },
            )
        )
        self.assertEqual(first.id, second.id)
        self.assertEqual(len(store.payments), 1)
        self.assertEqual(len(store.events), 1)

    async def test_every_significant_payload_field_participates_in_conflict(self):
        for changes in (
            {"amount": Decimal("13.00")},
            {"currency": "EUR"},
            {"description": "other"},
            {"metadata": {"order": 2}},
            {"webhook_url": "https://example.com/other"},
        ):
            with self.subTest(changes=changes):
                store = MemoryStore()
                create = CreatePayment(store.uow)
                await create.execute(command())
                with self.assertRaises(IdempotencyConflict):
                    await create.execute(command(**changes))
                self.assertEqual(len(store.payments), 1)
                self.assertEqual(len(store.events), 1)

    async def test_get_reads_current_payment_or_raises_not_found(self):
        store = MemoryStore()
        payment = await CreatePayment(store.uow).execute(command())
        store.payments[payment.id].status = PaymentStatus.FAILED
        found = await GetPayment(store.uow).execute(payment.id)
        self.assertEqual(found.status, PaymentStatus.FAILED)
        with self.assertRaises(PaymentNotFound):
            await GetPayment(store.uow).execute(uuid4())


class Gateway:
    def __init__(self, store, result=PaymentStatus.SUCCEEDED, error=None):
        self.store, self.result, self.error = store, result, error
        self.calls = 0

    async def charge(self, payment):
        assert self.store.active_transactions == 0, "Gateway inside transaction"
        self.calls += 1
        if self.error:
            raise self.error
        return self.result


class Webhook:
    def __init__(self, store, error=None):
        self.store, self.error = store, error
        self.notifications = []

    async def send(self, payment):
        assert self.store.active_transactions == 0, "Webhook inside transaction"
        assert self.store.payments[payment.id].status != PaymentStatus.PENDING
        self.notifications.append(payment.notification_id)
        if self.error:
            raise self.error


class ProcessingTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.store = MemoryStore()
        self.payment = await CreatePayment(self.store.uow).execute(command())
        row = next(iter(self.store.events.values()))
        self.event = PaymentCreated(row.event_id, row.payment_id)
        self.now = datetime(2026, 1, 1, tzinfo=UTC)
        self.gateway = Gateway(self.store)
        self.webhook = Webhook(self.store)

    def processor(self):
        return ProcessPayment(self.store.uow, self.gateway, self.webhook, clock=lambda: self.now)

    def retry_event(self, attempt):
        row = next(
            e
            for e in self.store.events.values()
            if e.payload.get("attempt") == attempt and e.routing_key == "payments.new"
        )
        return PaymentCreated(row.event_id, row.payment_id, attempt)

    async def test_complete_then_duplicate_has_no_external_effect(self):
        await self.processor().execute(self.event)
        await self.processor().execute(self.event)
        result = self.store.payments[self.payment.id]
        self.assertEqual(result.status, PaymentStatus.SUCCEEDED)
        self.assertEqual(result.processed_at, self.now)
        self.assertEqual(result.webhook_delivered_at, self.now)
        self.assertEqual(result.attempts, 1)
        self.assertEqual(self.gateway.calls, 1)
        self.assertEqual(self.webhook.notifications, [self.payment.notification_id])

    async def test_business_failure_is_terminal_and_notified(self):
        self.gateway.result = PaymentStatus.FAILED
        await self.processor().execute(self.event)
        self.assertEqual(self.store.payments[self.payment.id].status, PaymentStatus.FAILED)
        self.assertEqual(len(self.webhook.notifications), 1)
        self.assertEqual(len(self.store.events), 1)

    async def test_webhook_retry_after_restart_preserves_result_and_id(self):
        self.webhook.error = ConnectionError("receiver unavailable")
        await self.processor().execute(self.event)
        result = self.store.payments[self.payment.id]
        self.assertEqual(result.status, PaymentStatus.SUCCEEDED)
        self.assertEqual(result.next_attempt_at, self.now + timedelta(seconds=1))
        self.assertIsNone(result.webhook_delivered_at)
        self.now += timedelta(seconds=1)
        self.webhook.error = None
        await self.processor().execute(self.retry_event(2))  # fresh scenario simulates restart
        result = self.store.payments[self.payment.id]
        self.assertEqual(self.gateway.calls, 1)
        self.assertEqual(
            self.webhook.notifications, [self.payment.notification_id, self.payment.notification_id]
        )
        self.assertEqual(result.attempts, 2)
        self.assertIsNotNone(result.webhook_delivered_at)

    async def test_three_technical_failures_schedule_one_dlq_with_durable_backoff(self):
        self.gateway.error = ConnectionError("gateway down")
        event = self.event
        for attempt in (1, 2, 3):
            await self.processor().execute(event)
            result = self.store.payments[self.payment.id]
            self.assertEqual(result.attempts, attempt)
            if attempt < 3:
                wait = 2 ** (attempt - 1)
                self.assertEqual(result.next_attempt_at, self.now + timedelta(seconds=wait))
                self.now += timedelta(seconds=wait)
                event = self.retry_event(attempt + 1)
        result = self.store.payments[self.payment.id]
        self.assertTrue(result.dead_lettered)
        dlq = [e for e in self.store.events.values() if e.routing_key == "payments.dlq"]
        self.assertEqual(len(dlq), 1)
        self.assertEqual(dlq[0].payload["attempt"], 3)
        self.assertIn("gateway down", dlq[0].payload["reason"])
        await self.processor().execute(event)
        self.assertEqual(self.gateway.calls, 3)

    async def test_duplicate_of_scheduled_attempt_does_not_charge_or_enqueue_again(self):
        self.webhook.error = ConnectionError("down")
        await self.processor().execute(self.event)
        await self.processor().execute(self.event)
        self.assertEqual(self.gateway.calls, 1)
        self.assertEqual(len(self.webhook.notifications), 1)
        self.assertEqual(len(self.store.events), 2)
        self.assertEqual(self.store.payments[self.payment.id].attempts, 1)

    async def test_stale_event_cannot_consume_new_attempt(self):
        self.webhook.error = ConnectionError("down")
        await self.processor().execute(self.event)
        self.now += timedelta(seconds=1)
        await self.processor().execute(self.retry_event(2))
        await self.processor().execute(self.event)
        self.assertEqual(len(self.webhook.notifications), 2)
        self.assertEqual(len(self.store.events), 3)
        self.assertEqual(self.store.payments[self.payment.id].attempts, 2)

    async def test_interruption_after_result_commit_skips_gateway_on_retry(self):
        saved = self.store.payments[self.payment.id]
        saved.attempts = 1
        saved.status = PaymentStatus.SUCCEEDED
        saved.processed_at = self.now
        await self.processor().execute(self.event)
        self.now += timedelta(seconds=1)
        await self.processor().execute(self.retry_event(2))
        self.assertEqual(self.gateway.calls, 0)
        self.assertEqual(self.webhook.notifications, [self.payment.notification_id])

    async def test_receiver_accepted_but_local_delivery_commit_failed_repeats_stable_id(self):
        class FailDeliveryCommit(MemoryUow):
            async def commit(self):
                if any(p.webhook_delivered_at for p in self.payments.rows.values()):
                    raise ConnectionError("SQL commit failed after receiver accepted")
                await super().commit()

        processor = ProcessPayment(
            lambda: FailDeliveryCommit(self.store),
            self.gateway,
            self.webhook,
            clock=lambda: self.now,
        )
        with self.assertRaisesRegex(ConnectionError, "receiver accepted"):
            await processor.execute(self.event)
        self.assertIsNone(self.store.payments[self.payment.id].webhook_delivered_at)
        await self.processor().execute(self.event)
        self.now += timedelta(seconds=1)
        await self.processor().execute(self.retry_event(2))
        self.assertEqual(self.gateway.calls, 1)
        self.assertEqual(
            self.webhook.notifications, [self.payment.notification_id, self.payment.notification_id]
        )

    async def test_interrupted_third_attempt_reaches_dlq(self):
        self.store.payments[self.payment.id].attempts = 3
        await self.processor().execute(replace(self.event, attempt=3))
        self.assertTrue(self.store.payments[self.payment.id].dead_lettered)
        self.assertEqual(self.gateway.calls, 0)
        self.assertEqual(
            len([e for e in self.store.events.values() if e.routing_key == "payments.dlq"]), 1
        )

    async def test_retry_outbox_failure_propagates_and_does_not_lose_responsibility(self):
        self.webhook.error = ConnectionError("down")
        self.store.outbox_error = True
        with self.assertRaisesRegex(RuntimeError, "outbox unavailable"):
            await self.processor().execute(self.event)
        saved = self.store.payments[self.payment.id]
        self.assertEqual(saved.status, PaymentStatus.SUCCEEDED)
        self.assertIsNone(saved.next_attempt_at)
        self.assertIsNone(saved.webhook_delivered_at)
        self.store.outbox_error = False
        await self.processor().execute(self.event)
        self.assertEqual(self.retry_event(2).attempt, 2)
        self.assertEqual(self.gateway.calls, 1)

    async def test_cancellation_is_preserved_and_recovery_is_durable(self):
        self.gateway.error = asyncio.CancelledError()
        with self.assertRaises(asyncio.CancelledError):
            await self.processor().execute(self.event)
        self.assertEqual(self.store.payments[self.payment.id].attempts, 1)
        self.gateway.error = None
        await self.processor().execute(self.event)
        self.assertEqual(self.retry_event(2).attempt, 2)
        self.assertEqual(self.gateway.calls, 1)
        self.assertEqual(self.webhook.notifications, [])

    async def test_future_attempt_is_rejected_without_mutating_payment(self):
        with self.assertRaisesRegex(ValueError, "ahead"):
            await self.processor().execute(replace(self.event, attempt=2))
        self.assertEqual(self.store.payments[self.payment.id].attempts, 0)


class Publisher:
    def __init__(self, error=None):
        self.events = []
        self.error = error

    async def publish(self, event):
        self.events.append(event)
        if self.error:
            raise self.error


class OutboxTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.store = MemoryStore()
        await CreatePayment(self.store.uow).execute(command())

    async def test_publish_confirmation_precedes_mark_and_future_rows_wait(self):
        row = next(iter(self.store.events.values()))
        future = replace(row, event_id=uuid4(), available_at=row.created_at + timedelta(days=1))
        self.store.events[future.event_id] = future
        publisher = Publisher()
        count = await PublishOutboxBatch(self.store.uow, publisher).execute()
        self.assertEqual(count, 1)
        self.assertEqual(len(publisher.events), 1)
        self.assertIsNotNone(self.store.events[row.event_id].published_at)
        self.assertIsNone(self.store.events[future.event_id].published_at)

    async def test_publish_failure_leaves_outbox_unpublished_for_recovery(self):
        publisher = Publisher(ConnectionError("broker down"))
        with self.assertRaisesRegex(ConnectionError, "broker down"):
            await PublishOutboxBatch(self.store.uow, publisher).execute()
        self.assertIsNone(next(iter(self.store.events.values())).published_at)
        publisher.error = None
        await PublishOutboxBatch(self.store.uow, publisher).execute()
        self.assertEqual(publisher.events[0].event_id, publisher.events[1].event_id)
        self.assertIsNotNone(next(iter(self.store.events.values())).published_at)

    async def test_confirmed_publish_followed_by_commit_failure_republishes_same_event(self):
        class FailCommit(MemoryUow):
            async def commit(self):
                raise ConnectionError("SQL commit failed")

        publisher = Publisher()
        with self.assertRaisesRegex(ConnectionError, "SQL commit failed"):
            await PublishOutboxBatch(lambda: FailCommit(self.store), publisher).execute()
        self.assertIsNone(next(iter(self.store.events.values())).published_at)
        await PublishOutboxBatch(self.store.uow, publisher).execute()
        self.assertEqual(len(publisher.events), 2)
        self.assertEqual(publisher.events[0].event_id, publisher.events[1].event_id)
