from dataclasses import dataclass
from typing import Any
from uuid import UUID


@dataclass(frozen=True)
class PaymentCreated:
    event_id: UUID
    payment_id: UUID
    attempt: int = 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": str(self.event_id),
            "payment_id": str(self.payment_id),
            "attempt": self.attempt,
        }
