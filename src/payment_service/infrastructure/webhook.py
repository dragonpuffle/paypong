import httpx

from payment_service.domain.models import Payment


class HttpWebhookSender:
    def __init__(self, client: httpx.AsyncClient) -> None:
        self.client = client

    async def send(self, payment: Payment) -> None:
        response = await self.client.post(
            payment.webhook_url,
            headers={"Idempotency-Key": str(payment.notification_id)},
            json={
                "notification_id": str(payment.notification_id),
                "payment_id": str(payment.id),
                "status": payment.status.value,
                "amount": str(payment.amount),
                "currency": payment.currency,
                "processed_at": payment.processed_at.isoformat() if payment.processed_at else None,
            },
        )
        response.raise_for_status()
