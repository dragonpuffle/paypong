import hmac
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from paypong.config import Settings
from paypong.domain.errors import IdempotencyConflict, PaymentNotFound
from paypong.presentation.http.routes import router


def create_app(settings: Settings | None = None, *, uow_factory=None) -> FastAPI:
    config = settings or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if uow_factory is not None:
            app.state.uow_factory = uow_factory
            yield
            return
        from paypong.bootstrap import ApiResources

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

    app.include_router(router)
    return app
