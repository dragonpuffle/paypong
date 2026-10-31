from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4


def utcnow() -> datetime:
    return datetime.now(UTC)


class PaymentStatus(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


@dataclass
class Payment:
    id: UUID
    amount: Decimal
    currency: str
    description: str
    metadata: dict[str, Any]
    idempotency_key: str
    webhook_url: str
    request_hash: str
    status: PaymentStatus = PaymentStatus.PENDING
    created_at: datetime = field(default_factory=utcnow)
    processed_at: datetime | None = None
    notification_id: UUID = field(default_factory=uuid4)
    webhook_delivered_at: datetime | None = None
    attempts: int = 0
    next_attempt_at: datetime | None = None
    last_error: str | None = None
    dead_lettered: bool = False


@dataclass
class OutboxEvent:
    event_id: UUID
    payment_id: UUID | None
    payload: dict[str, Any]
    routing_key: str = "payments.new"
    available_at: datetime = field(default_factory=utcnow)
    created_at: datetime = field(default_factory=utcnow)
    published_at: datetime | None = None
