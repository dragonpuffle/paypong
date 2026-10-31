from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    api_key: SecretStr = Field(min_length=16)
    debug: bool = True
    database_url: str = "postgresql+asyncpg://paypong:paypong@localhost:5432/paypong"
    rabbitmq_url: str = "amqp://paypong:paypong@localhost:5672/"
    publish_timeout: float = Field(default=5, gt=0, le=30)
    webhook_timeout: float = Field(default=10, gt=0, le=60)
    outbox_poll_seconds: float = Field(default=0.5, gt=0)
    outbox_batch_size: int = Field(default=20, ge=1, le=100)
    retry_base_seconds: float = Field(default=1, gt=0, le=60)
