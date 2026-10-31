from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class PaymentRow(Base):
    __tablename__ = "payments"
    __table_args__ = (
        CheckConstraint("amount > 0", name="ck_payments_positive_amount"),
        CheckConstraint("currency IN ('RUB', 'USD', 'EUR')", name="ck_payments_currency"),
        CheckConstraint("status IN ('pending', 'succeeded', 'failed')", name="ck_payments_status"),
        CheckConstraint("attempts BETWEEN 0 AND 3", name="ck_payments_attempts"),
        CheckConstraint("length(idempotency_key) BETWEEN 1 AND 255", name="ck_payments_key_length"),
        CheckConstraint("jsonb_typeof(metadata) = 'object'", name="ck_payments_metadata_object"),
        CheckConstraint(
            "(status = 'pending' AND processed_at IS NULL) OR "
            "(status != 'pending' AND processed_at IS NOT NULL)",
            name="ck_payments_processed_status",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    currency: Mapped[str] = mapped_column(String(3))
    description: Mapped[str] = mapped_column(Text)
    metadata_json: Mapped[dict[str, Any]] = mapped_column("metadata", JSONB)
    idempotency_key: Mapped[str] = mapped_column(String(255), unique=True)
    webhook_url: Mapped[str] = mapped_column(Text)
    request_hash: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(16), server_default="pending")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()")
    )
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    notification_id: Mapped[UUID] = mapped_column(unique=True)
    webhook_delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempts: Mapped[int] = mapped_column(Integer, server_default="0")
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(Text)
    dead_lettered: Mapped[bool] = mapped_column(Boolean, server_default=text("false"))


class OutboxRow(Base):
    __tablename__ = "outbox"
    __table_args__ = (
        Index(
            "ix_outbox_due",
            "available_at",
            "created_at",
            postgresql_where=text("published_at IS NULL"),
        ),
    )

    event_id: Mapped[UUID] = mapped_column(primary_key=True)
    payment_id: Mapped[UUID | None] = mapped_column(ForeignKey("payments.id"))
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    routing_key: Mapped[str] = mapped_column(String(255))
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()")
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
