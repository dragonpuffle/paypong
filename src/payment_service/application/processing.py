import logging
from collections.abc import Callable
from datetime import datetime, timedelta
from uuid import uuid5

from payment_service.application.contracts import PaymentCreated
from payment_service.application.ports import PaymentGateway, UowFactory, WebhookSender
from payment_service.domain.errors import PaymentNotFound
from payment_service.domain.models import OutboxEvent, PaymentStatus, utcnow

logger = logging.getLogger(__name__)


class ProcessPayment:
    """Reserve attempts durably and resume stages; one active consumer is required."""

    def __init__(
        self,
        uow_factory: UowFactory,
        gateway: PaymentGateway,
        webhook: WebhookSender,
        *,
        clock: Callable[[], datetime] = utcnow,
        retry_base_seconds: float = 1,
    ) -> None:
        if retry_base_seconds <= 0:
            raise ValueError("retry_base_seconds must be positive")
        self._uow_factory = uow_factory
        self._gateway = gateway
        self._webhook = webhook
        self._clock = clock
        self._retry_base_seconds = retry_base_seconds

    async def execute(self, event: PaymentCreated) -> None:
        if not 1 <= event.attempt <= 3:
            raise ValueError("Invalid processing attempt")
        interrupted = False
        async with self._uow_factory() as uow:
            payment = await uow.payments.get(event.payment_id, for_update=True)
            if payment is None:
                raise PaymentNotFound(str(event.payment_id))
            if payment.webhook_delivered_at is not None or payment.dead_lettered:
                return
            if event.attempt < payment.attempts:
                return
            if event.attempt == payment.attempts:
                if payment.next_attempt_at is not None:
                    return  # Responsibility already transferred to a durable retry.
                interrupted = True
            elif event.attempt != payment.attempts + 1:
                raise ValueError("Attempt is ahead of durable payment state")
            else:
                if payment.next_attempt_at and payment.next_attempt_at > self._clock():
                    raise ValueError("Retry delivered before its scheduled time")
                payment.attempts = event.attempt
                payment.next_attempt_at = None
                await uow.payments.save(payment)
                await uow.commit()

        if interrupted:
            await self._failure(event, "interrupted", RuntimeError("Previous attempt interrupted"))
            return

        if payment.status == PaymentStatus.PENDING:
            try:
                status = await self._gateway.charge(payment)
                if status not in (PaymentStatus.SUCCEEDED, PaymentStatus.FAILED):
                    raise ValueError("Gateway returned a non-terminal status")
            except Exception as exc:
                await self._failure(event, "gateway", exc)
                return
            async with self._uow_factory() as uow:
                payment = await uow.payments.get(event.payment_id, for_update=True)
                if payment is None:
                    raise PaymentNotFound(str(event.payment_id))
                payment.status = status
                payment.processed_at = self._clock()
                await uow.payments.save(payment)
                await uow.commit()

        try:
            await self._webhook.send(payment)
        except Exception as exc:
            await self._failure(event, "webhook", exc)
            return
        async with self._uow_factory() as uow:
            payment = await uow.payments.get(event.payment_id, for_update=True)
            if payment is None:
                raise PaymentNotFound(str(event.payment_id))
            payment.webhook_delivered_at = self._clock()
            payment.next_attempt_at = None
            payment.last_error = None
            await uow.payments.save(payment)
            await uow.commit()
        logger.info(
            "payment_id=%s event_id=%s stage=complete attempt=%s",
            event.payment_id,
            event.event_id,
            event.attempt,
        )

    async def _failure(self, event: PaymentCreated, stage: str, error: Exception) -> None:
        reason = f"{stage}: {type(error).__name__}: {error}"[:2000]
        async with self._uow_factory() as uow:
            payment = await uow.payments.get(event.payment_id, for_update=True)
            if payment is None:
                raise PaymentNotFound(str(event.payment_id))
            if payment.webhook_delivered_at or payment.dead_lettered or payment.next_attempt_at:
                return
            payment.last_error = reason
            if event.attempt >= 3:
                payment.dead_lettered = True
                outgoing = OutboxEvent(
                    event_id=uuid5(event.event_id, "dlq"),
                    payment_id=payment.id,
                    payload={**event.to_dict(), "reason": reason, "stage": stage},
                    routing_key="payments.dlq",
                    available_at=self._clock(),
                )
            else:
                payment.next_attempt_at = self._clock() + timedelta(
                    seconds=self._retry_base_seconds * 2 ** (event.attempt - 1),
                )
                retry = PaymentCreated(
                    event_id=uuid5(event.event_id, f"attempt:{event.attempt + 1}"),
                    payment_id=payment.id,
                    attempt=event.attempt + 1,
                )
                outgoing = OutboxEvent(
                    event_id=retry.event_id,
                    payment_id=payment.id,
                    payload=retry.to_dict(),
                    available_at=payment.next_attempt_at,
                )
            await uow.payments.save(payment)
            await uow.outbox.add(outgoing)
            await uow.commit()
        logger.warning(
            "payment_id=%s event_id=%s stage=%s attempt=%s reason=%s",
            event.payment_id,
            event.event_id,
            stage,
            event.attempt,
            reason,
        )
