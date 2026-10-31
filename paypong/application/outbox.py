from collections.abc import Callable
from datetime import datetime

from paypong.application.ports import EventPublisher, UowFactory
from paypong.domain.models import utcnow


class PublishOutboxBatch:
    def __init__(
        self,
        uow_factory: UowFactory,
        publisher: EventPublisher,
        *,
        batch_size: int = 20,
        clock: Callable[[], datetime] = utcnow,
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        self._uow_factory = uow_factory
        self._publisher = publisher
        self._batch_size = batch_size
        self._clock = clock

    async def execute(self) -> int:
        async with self._uow_factory() as uow:
            events = await uow.outbox.pending_batch(self._batch_size)
            for event in events:
                await self._publisher.publish(event)
                await uow.outbox.mark_published(event.event_id, self._clock())
            await uow.commit()
        return len(events)
