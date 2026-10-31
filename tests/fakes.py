from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime
from uuid import UUID

from payment_service.domain.models import OutboxEvent, Payment, utcnow


@dataclass
class MemoryStore:
    payments: dict[UUID, Payment] = field(default_factory=dict)
    events: dict[UUID, OutboxEvent] = field(default_factory=dict)
    outbox_error: bool = False
    active_transactions: int = 0

    def uow(self):
        return MemoryUow(self)


class MemoryPayments:
    def __init__(self, rows):
        self.rows = rows

    async def insert_if_absent(self, payment: Payment) -> bool:
        if any(p.idempotency_key == payment.idempotency_key for p in self.rows.values()):
            return False
        self.rows[payment.id] = deepcopy(payment)
        return True

    async def find_by_key(self, key: str) -> Payment | None:
        return next((deepcopy(p) for p in self.rows.values() if p.idempotency_key == key), None)

    async def get(self, payment_id: UUID, *, for_update: bool = False) -> Payment | None:
        return deepcopy(self.rows.get(payment_id))

    async def save(self, payment: Payment) -> None:
        self.rows[payment.id] = deepcopy(payment)


class MemoryOutbox:
    def __init__(self, rows, store):
        self.rows = rows
        self.store = store

    async def add(self, event: OutboxEvent) -> None:
        if self.store.outbox_error:
            raise RuntimeError("outbox unavailable")
        self.rows.setdefault(event.event_id, deepcopy(event))

    async def pending_batch(self, limit: int) -> list[OutboxEvent]:
        return [
            deepcopy(e)
            for e in self.rows.values()
            if e.published_at is None and e.available_at <= utcnow()
        ][:limit]

    async def mark_published(self, event_id: UUID, at: datetime) -> None:
        self.rows[event_id].published_at = at


class MemoryUow:
    def __init__(self, store: MemoryStore):
        self.store = store

    async def __aenter__(self):
        self.store.active_transactions += 1
        self.payments = MemoryPayments(deepcopy(self.store.payments))
        self.outbox = MemoryOutbox(deepcopy(self.store.events), self.store)
        return self

    async def __aexit__(self, *args):
        self.store.active_transactions -= 1

    async def commit(self):
        self.store.payments = deepcopy(self.payments.rows)
        self.store.events = deepcopy(self.outbox.rows)
