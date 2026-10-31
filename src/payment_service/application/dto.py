from dataclasses import dataclass
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class CreatePaymentCommand:
    amount: Decimal
    currency: str
    description: str
    metadata: dict[str, Any]
    idempotency_key: str
    webhook_url: str
