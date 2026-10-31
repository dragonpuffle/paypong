from dataclasses import asdict
from datetime import datetime
from uuid import UUID

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from paypong.domain.models import OutboxEvent, Payment, PaymentStatus
from paypong.infrastructure.postgres.models import OutboxRow, PaymentRow


def payment_values(payment: Payment) -> dict:
    values = asdict(payment)
    values["status"] = payment.status.value
    # Use the physical column name for Core insert/update statements.
    return values


def to_payment(row: PaymentRow) -> Payment:
    return Payment(
        id=row.id,
        amount=row.amount,
        currency=row.currency,
        description=row.description,
        metadata=row.metadata_json,
        idempotency_key=row.idempotency_key,
        webhook_url=row.webhook_url,
        request_hash=row.request_hash,
        status=PaymentStatus(row.status),
        created_at=row.created_at,
        processed_at=row.processed_at,
        notification_id=row.notification_id,
        webhook_delivered_at=row.webhook_delivered_at,
        attempts=row.attempts,
        next_attempt_at=row.next_attempt_at,
        last_error=row.last_error,
        dead_lettered=row.dead_lettered,
    )


class SqlPaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def insert_if_absent(self, payment: Payment) -> bool:
        result = await self.session.execute(
            insert(PaymentRow.__table__)
            .values(**payment_values(payment))
            .on_conflict_do_nothing(index_elements=[PaymentRow.idempotency_key])
            .returning(PaymentRow.id)
        )
        return result.scalar_one_or_none() is not None

    async def find_by_key(self, key: str) -> Payment | None:
        row = await self.session.scalar(select(PaymentRow).where(PaymentRow.idempotency_key == key))
        return to_payment(row) if row is not None else None

    async def get(self, payment_id: UUID, *, for_update: bool = False) -> Payment | None:
        statement = select(PaymentRow).where(PaymentRow.id == payment_id)
        if for_update:
            statement = statement.with_for_update()
        # Re-read state even when the same row was loaded earlier in this transaction.
        row = await self.session.scalar(statement.execution_options(populate_existing=True))
        return to_payment(row) if row is not None else None

    async def save(self, payment: Payment) -> None:
        await self.session.execute(
            update(PaymentRow.__table__)
            .where(PaymentRow.id == payment.id)
            .values(**payment_values(payment))
        )


class SqlOutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def add(self, event: OutboxEvent) -> None:
        await self.session.execute(
            insert(OutboxRow.__table__)
            .values(**asdict(event))
            .on_conflict_do_nothing(index_elements=[OutboxRow.event_id])
        )

    async def pending_batch(self, limit: int) -> list[OutboxEvent]:
        if limit <= 0:
            raise ValueError("Outbox batch limit must be positive")
        rows = await self.session.scalars(
            select(OutboxRow)
            .where(OutboxRow.published_at.is_(None), OutboxRow.available_at <= func.now())
            .order_by(OutboxRow.available_at, OutboxRow.created_at, OutboxRow.event_id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return [
            OutboxEvent(
                event_id=row.event_id,
                payment_id=row.payment_id,
                payload=row.payload,
                routing_key=row.routing_key,
                available_at=row.available_at,
                created_at=row.created_at,
                published_at=row.published_at,
            )
            for row in rows
        ]

    async def mark_published(self, event_id: UUID, at: datetime) -> None:
        await self.session.execute(
            update(OutboxRow).where(OutboxRow.event_id == event_id).values(published_at=at)
        )
