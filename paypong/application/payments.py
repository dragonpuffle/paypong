import hashlib
import json
from decimal import Decimal
from math import isfinite
from typing import Any
from uuid import UUID, uuid4

from paypong.application.contracts import PaymentCreated
from paypong.application.dto import CreatePaymentCommand
from paypong.application.ports import UowFactory
from paypong.domain.errors import IdempotencyConflict, PaymentNotFound
from paypong.domain.models import OutboxEvent, Payment


def _canonical_json(value: Any) -> str:
    """Canonical JSON, including numeric equivalence and recursively sorted objects."""
    if value is None or isinstance(value, (str, bool)):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not isfinite(value):
            raise ValueError("JSON metadata must contain finite numbers")
        if value == 0:
            return "0"
        number = format(Decimal(str(value)), "f")
        return number.rstrip("0").rstrip(".") if "." in number else number
    if isinstance(value, list):
        return "[" + ",".join(_canonical_json(item) for item in value) + "]"
    if isinstance(value, dict) and all(isinstance(k, str) for k in value):
        return (
            "{"
            + ",".join(
                json.dumps(k, ensure_ascii=False) + ":" + _canonical_json(value[k])
                for k in sorted(value)
            )
            + "}"
        )
    raise ValueError("Metadata must be JSON")


def request_fingerprint(command: CreatePaymentCommand) -> str:
    data = {
        "amount": format(command.amount.quantize(Decimal("0.01")), ".2f"),
        "currency": command.currency,
        "description": command.description,
        "metadata": command.metadata,
        "webhook_url": command.webhook_url,
    }
    return hashlib.sha256(_canonical_json(data).encode("utf-8")).hexdigest()


class CreatePayment:
    def __init__(self, uow_factory: UowFactory) -> None:
        self._uow_factory = uow_factory

    async def execute(self, command: CreatePaymentCommand) -> Payment:
        payment = Payment(id=uuid4(), request_hash=request_fingerprint(command), **command.__dict__)
        async with self._uow_factory() as uow:
            inserted = await uow.payments.insert_if_absent(payment)
            if not inserted:
                existing = await uow.payments.find_by_key(command.idempotency_key)
                if existing is None:
                    raise RuntimeError("Idempotency conflict without an existing payment")
                if existing.request_hash != payment.request_hash:
                    raise IdempotencyConflict(command.idempotency_key)
                return existing
            event = PaymentCreated(event_id=uuid4(), payment_id=payment.id)
            await uow.outbox.add(
                OutboxEvent(
                    event_id=event.event_id,
                    payment_id=payment.id,
                    payload=event.to_dict(),
                )
            )
            await uow.commit()
        return payment


class GetPayment:
    def __init__(self, uow_factory: UowFactory) -> None:
        self._uow_factory = uow_factory

    async def execute(self, payment_id: UUID) -> Payment:
        async with self._uow_factory() as uow:
            payment = await uow.payments.get(payment_id)
            if payment is None:
                raise PaymentNotFound(str(payment_id))
            return payment
