from types import TracebackType
from typing import Self

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.infrastructure.postgres.repositories import (
    SqlOutboxRepository,
    SqlPaymentRepository,
)


class SqlUnitOfWork:
    """Own one session and its transaction; repositories never commit independently."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self.session_factory = session_factory
        self._committed = False

    async def __aenter__(self) -> Self:
        self.session = self.session_factory()
        self._committed = False
        self.payments = SqlPaymentRepository(self.session)
        self.outbox = SqlOutboxRepository(self.session)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        try:
            if exc_type is not None or not self._committed:
                await self.session.rollback()
        finally:
            await self.session.close()

    async def commit(self) -> None:
        await self.session.commit()
        self._committed = True
