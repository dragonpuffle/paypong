import asyncio
import hashlib
import random

from payment_service.domain.models import Payment, PaymentStatus


class SimulatedGateway:
    async def charge(self, payment: Payment) -> PaymentStatus:
        await asyncio.sleep(random.uniform(2, 5))
        bucket = int.from_bytes(hashlib.sha256(payment.id.bytes).digest(), "big") % 10
        return PaymentStatus.FAILED if bucket == 0 else PaymentStatus.SUCCEEDED
