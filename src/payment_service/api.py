import asyncio
import hmac
from contextlib import asynccontextmanager
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from payment_service.application.dto import CreatePaymentCommand
from payment_service.application.payments import CreatePayment, GetPayment
from payment_service.config import Settings
from payment_service.domain.errors import IdempotencyConflict, PaymentNotFound
from payment_service.presentation.http.schemas import (
    CreatedPaymentResponse,
    CreatePaymentBody,
    PaymentResponse,
)


def create_app(settings: Settings | None = None, *, uow_factory=None) -> FastAPI:
    config = settings or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if uow_factory is not None:
            app.state.uow_factory = uow_factory
            yield
            return
        from payment_service.bootstrap import ApiResources

        async with ApiResources(config) as resources:
            app.state.uow_factory = resources.uow_factory
            app.state.publisher_task = resources.publisher_task
            yield

    async def authenticate(x_api_key: Annotated[str | None, Header()] = None) -> None:
        if x_api_key is None or not hmac.compare_digest(
            x_api_key.encode(), config.api_key.get_secret_value().encode()
        ):
            raise HTTPException(401, "Invalid API key")

    app = FastAPI(
        title="Paypong",
        lifespan=lifespan,
        dependencies=[Depends(authenticate)],
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    if uow_factory is not None:
        app.state.uow_factory = uow_factory

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, exc: RequestValidationError):
        # Invalid JSON numbers are not serializable in the default error's input.
        # Return useful locations/messages without echoing untrusted input values.
        errors = [{"type": e["type"], "loc": list(e["loc"]), "msg": e["msg"]} for e in exc.errors()]
        return JSONResponse(status_code=422, content={"detail": errors})

    @app.exception_handler(IdempotencyConflict)
    async def conflict(request: Request, exc: IdempotencyConflict):
        return JSONResponse(status_code=409, content={"detail": "Idempotency key payload conflict"})

    @app.exception_handler(PaymentNotFound)
    async def missing(request: Request, exc: PaymentNotFound):
        return JSONResponse(status_code=404, content={"detail": "Payment not found"})

    @app.get("/health", include_in_schema=False)
    async def health(request: Request):
        task: asyncio.Task | None = getattr(request.app.state, "publisher_task", None)
        if task is not None and task.done():
            raise HTTPException(503, "Outbox publisher stopped")
        return {"status": "ok"}

    @app.post("/api/v1/payments", status_code=202, response_model=CreatedPaymentResponse)
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

    @app.get("/api/v1/payments/{payment_id}", response_model=PaymentResponse)
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

    return app
