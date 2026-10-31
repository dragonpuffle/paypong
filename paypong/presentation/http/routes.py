import asyncio
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Request

from paypong.application.dto import CreatePaymentCommand
from paypong.application.payments import CreatePayment, GetPayment
from paypong.presentation.http.schemas import (
    CreatedPaymentResponse,
    CreatePaymentBody,
    PaymentResponse,
)

router = APIRouter()


@router.get("/health", include_in_schema=False)
async def health(request: Request):
    task: asyncio.Task | None = getattr(request.app.state, "publisher_task", None)
    if task is not None and task.done():
        raise HTTPException(503, "Outbox publisher stopped")
    return {"status": "ok"}


@router.post("/api/v1/payments", status_code=202, response_model=CreatedPaymentResponse)
async def create_payment(
    body: CreatePaymentBody,
    request: Request,
    idempotency_key: Annotated[str, Header(min_length=1, max_length=255, pattern=r"^\S+$")],
):
    payment = await CreatePayment(request.app.state.uow_factory).execute(
        CreatePaymentCommand(
            amount=body.amount,
            currency=body.currency,
            description=body.description,
            metadata=body.metadata,
            webhook_url=str(body.webhook_url),
            idempotency_key=idempotency_key,
        )
    )
    return CreatedPaymentResponse(
        payment_id=payment.id,
        status=payment.status,
        created_at=payment.created_at,
    )


@router.get("/api/v1/payments/{payment_id}", response_model=PaymentResponse)
async def get_payment(payment_id: UUID, request: Request):
    payment = await GetPayment(request.app.state.uow_factory).execute(payment_id)
    return PaymentResponse(
        payment_id=payment.id,
        status=payment.status,
        created_at=payment.created_at,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.metadata,
        webhook_url=payment.webhook_url,
        processed_at=payment.processed_at,
    )
