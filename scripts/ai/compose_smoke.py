"""Run in a disposable Python container attached to the Compose network."""

import json
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer
from uuid import uuid4

notifications = []


class Receiver(BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers["Content-Length"]))
        notifications.append((json.loads(body), self.headers.get("Idempotency-Key")))
        self.send_response(200)
        self.end_headers()


def request(path, payload=None, key=None):
    headers = {"X-API-Key": "local-development-key-change-me"}
    if key:
        headers["Idempotency-Key"] = key
    data = json.dumps(payload).encode() if payload is not None else None
    headers["Content-Type"] = "application/json"
    try:
        with urllib.request.urlopen(
            urllib.request.Request("http://api:8000" + path, data=data, headers=headers),
            timeout=10,
        ) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as exc:
        return exc.code, json.load(exc)


def main():
    server = HTTPServer(("0.0.0.0", 9000), Receiver)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        key = str(uuid4())
        payload = {
            "amount": "123.45",
            "currency": "RUB",
            "description": "Compose smoke",
            "metadata": {"smoke": key},
            "webhook_url": "http://paypong-smoke:9000/webhook",
        }
        status, created = request("/api/v1/payments", payload, key)
        assert status == 202, (status, created)
        print("created payment=" + created["payment_id"], flush=True)
        status, replay = request("/api/v1/payments", payload, key)
        assert status == 202 and replay["payment_id"] == created["payment_id"]
        status, _ = request("/api/v1/payments", {**payload, "amount": "123.46"}, key)
        assert status == 409
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            status, detail = request("/api/v1/payments/" + created["payment_id"])
            assert status == 200
            if detail["status"] != "pending" and notifications:
                break
            time.sleep(0.2)
        else:
            raise TimeoutError("No terminal payment and webhook in Compose")
        notification, notification_key = notifications[0]
        assert notification["payment_id"] == created["payment_id"]
        assert notification["status"] == detail["status"]
        assert notification["notification_id"] == notification_key
        assert notification["amount"] == "123.45"
        print(
            json.dumps(
                {
                    "smoke": "passed",
                    "payment_id": created["payment_id"],
                    "status": detail["status"],
                    "webhooks": len(notifications),
                }
            ),
            flush=True,
        )
    finally:
        server.shutdown()


if __name__ == "__main__":
    main()
