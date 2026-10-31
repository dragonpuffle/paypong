import json
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, JsonValue, field_validator

from paypong.domain.models import PaymentStatus


class CreatePaymentBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    amount: Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2, allow_inf_nan=False)]
    currency: Literal["RUB", "USD", "EUR"]
    description: str = Field(max_length=2000)
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    webhook_url: HttpUrl

    @field_validator("metadata")
    @classmethod
    def finite_json(cls, value: dict[str, JsonValue]) -> dict[str, JsonValue]:
        json.dumps(value, allow_nan=False)
        return value

    @field_validator("webhook_url")
    @classmethod
    def no_credentials(cls, value: HttpUrl) -> HttpUrl:
        if value.username or value.password or value.fragment:
            raise ValueError("webhook URL must not contain credentials or fragment")
        return value


class CreatedPaymentResponse(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    created_at: datetime


class PaymentResponse(CreatedPaymentResponse):
    amount: Decimal
    currency: str
    description: str
    metadata: dict[str, JsonValue]
    webhook_url: str
    processed_at: datetime | None
