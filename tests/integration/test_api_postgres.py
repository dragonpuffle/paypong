import asyncio

import httpx
from sqlalchemy import func, select

from paypong.api import create_app
from paypong.config import Settings
from paypong.infrastructure.postgres.models import OutboxRow, PaymentRow
from paypong.infrastructure.postgres.uow import SqlUnitOfWork
from tests.http.test_api import BODY, KEY
from tests.integration.test_postgres import database as database


async def test_concurrent_http_idempotency_on_postgres(database):
    _, sessions = database

    def factory():
        return SqlUnitOfWork(sessions)

    app = create_app(Settings(api_key=KEY), uow_factory=factory)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as api:
        headers = {"X-API-Key": KEY, "Idempotency-Key": "concurrent-order"}
        results = await asyncio.gather(
            *[api.post("/api/v1/payments", json=BODY, headers=headers) for _ in range(8)]
        )
        assert all(r.status_code == 202 for r in results)
        assert len({r.json()["payment_id"] for r in results}) == 1
        conflict = await api.post(
            "/api/v1/payments", json={**BODY, "amount": "99"}, headers=headers
        )
        assert conflict.status_code == 409
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(PaymentRow)) == 1
        assert await session.scalar(select(func.count()).select_from(OutboxRow)) == 1
