"""Create payments and transactional outbox.

Revision ID: 0001
Revises:
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "payments",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("amount", sa.Numeric(18, 2), nullable=False),
        sa.Column("currency", sa.String(3), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("metadata", postgresql.JSONB(), nullable=False),
        sa.Column("idempotency_key", sa.String(255), nullable=False),
        sa.Column("webhook_url", sa.Text(), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default="pending"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("notification_id", sa.Uuid(), nullable=False),
        sa.Column("webhook_delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("dead_lettered", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("idempotency_key"),
        sa.UniqueConstraint("notification_id"),
        sa.CheckConstraint("amount > 0", name="ck_payments_positive_amount"),
        sa.CheckConstraint("currency IN ('RUB', 'USD', 'EUR')", name="ck_payments_currency"),
        sa.CheckConstraint(
            "status IN ('pending', 'succeeded', 'failed')", name="ck_payments_status"
        ),
        sa.CheckConstraint("attempts BETWEEN 0 AND 3", name="ck_payments_attempts"),
        sa.CheckConstraint(
            "length(idempotency_key) BETWEEN 1 AND 255", name="ck_payments_key_length"
        ),
        sa.CheckConstraint("jsonb_typeof(metadata) = 'object'", name="ck_payments_metadata_object"),
        sa.CheckConstraint(
            "(status = 'pending' AND processed_at IS NULL) OR "
            "(status != 'pending' AND processed_at IS NOT NULL)",
            name="ck_payments_processed_status",
        ),
    )
    op.create_table(
        "outbox",
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column("payment_id", sa.Uuid(), nullable=True),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("routing_key", sa.String(255), nullable=False),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("event_id"),
        sa.ForeignKeyConstraint(["payment_id"], ["payments.id"]),
    )
    op.create_index(
        "ix_outbox_due",
        "outbox",
        ["available_at", "created_at"],
        postgresql_where=sa.text("published_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_outbox_due", table_name="outbox")
    op.drop_table("outbox")
    op.drop_table("payments")
