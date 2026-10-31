from uuid import uuid4

import httpx
import pytest

from paypong.api import create_app
from paypong.config import Settings
from tests.fakes import MemoryStore

KEY = "test-api-key-123456"
BODY = {
    "amount": "12.30",
    "currency": "RUB",
    "description": "order",
    "metadata": {"order": 12},
    "webhook_url": "https://example.com/hook",
}


@pytest.fixture
async def api():
    store = MemoryStore()
    app = create_app(Settings(api_key=KEY), uow_factory=store.uow)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as c:
        yield c, store


async def test_create_get_replay_conflict(api):
    client, store = api
    headers = {"X-API-Key": KEY, "Idempotency-Key": "order-1"}
    response = await client.post("/api/v1/payments", headers=headers, json=BODY)
    assert response.status_code == 202, response.text
    first = response.json()
    assert first["status"] == "pending"
    assert len(store.payments) == len(store.events) == 1
    replay = await client.post(
        "/api/v1/payments", headers=headers, json={**BODY, "amount": "12.300"}
    )
    assert replay.status_code == 202
    assert replay.json() == first
    details = await client.get(f"/api/v1/payments/{first['payment_id']}", headers=headers)
    assert details.status_code == 200
    assert details.json()["amount"] == "12.30"
    assert details.json()["metadata"] == BODY["metadata"]
    assert "idempotency_key" not in details.json()
    conflict = await client.post(
        "/api/v1/payments", headers=headers, json={**BODY, "metadata": {"order": 13}}
    )
    assert conflict.status_code == 409
    missing = await client.get(f"/api/v1/payments/{uuid4()}", headers=headers)
    assert missing.status_code == 404


@pytest.mark.parametrize(
    "path,method",
    [("/health", "get"), ("/api/v1/payments", "post"), (f"/api/v1/payments/{uuid4()}", "get")],
)
async def test_auth(api, path, method):
    client, _ = api
    args = {"json": BODY} if method == "post" else {}
    for headers in ({}, {"X-API-Key": "bad"}):
        response = await getattr(client, method)(path, headers=headers, **args)
        assert response.status_code == 401


@pytest.mark.parametrize(
    "change",
    [
        {"amount": "0"},
        {"amount": "-1"},
        {"amount": "1.001"},
        {"amount": "10000000000000000"},
        {"amount": "NaN"},
        {"currency": "BTC"},
        {"webhook_url": "ftp://example.com"},
        {"webhook_url": "https://user:pass@example.com"},
        {"metadata": []},
    ],
)
async def test_validation(api, change):
    client, store = api
    result = await client.post(
        "/api/v1/payments",
        json={**BODY, **change},
        headers={"X-API-Key": KEY, "Idempotency-Key": "valid"},
    )
    assert result.status_code == 422
    assert not store.payments


async def test_required_idempotency_header(api):
    client, _ = api
    response = await client.post("/api/v1/payments", json=BODY, headers={"X-API-Key": KEY})
    assert response.status_code == 422


@pytest.mark.parametrize("number", ["NaN", "Infinity", "1e309"])
async def test_nonfinite_metadata_rejected_at_http_boundary(api, number):
    client, store = api
    import json

    content = json.dumps(BODY).replace('"order": 12', f'"order": {number}')
    response = await client.post(
        "/api/v1/payments",
        content=content,
        headers={
            "X-API-Key": KEY,
            "Idempotency-Key": "finite-json",
            "Content-Type": "application/json",
        },
    )
    assert response.status_code == 422
    assert not store.payments


async def test_no_public_documentation(api):
    client, _ = api
    for path in ("/docs", "/redoc", "/openapi.json"):
        assert (await client.get(path)).status_code == 404
