import asyncio
import logging
from contextlib import AsyncExitStack

import httpx
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from payment_service.application.outbox import PublishOutboxBatch
from payment_service.application.processing import ProcessPayment
from payment_service.config import Settings
from payment_service.infrastructure.gateway import SimulatedGateway
from payment_service.infrastructure.postgres.uow import SqlUnitOfWork
from payment_service.infrastructure.rabbitmq.publisher import RabbitEventPublisher
from payment_service.infrastructure.rabbitmq.topology import declare_topology, make_broker
from payment_service.infrastructure.webhook import HttpWebhookSender
from payment_service.presentation.consumer import ConsumerHandler, register_consumer

logger = logging.getLogger(__name__)


class ApiResources:
    def __init__(self, config: Settings) -> None:
        self.config = config
        self.stack = AsyncExitStack()

    async def __aenter__(self):
        await self.stack.__aenter__()
        try:
            self.engine = create_async_engine(self.config.database_url, pool_pre_ping=True)
            self.stack.push_async_callback(self.engine.dispose)
            sessions = async_sessionmaker(self.engine, expire_on_commit=False)
            self.uow_factory = lambda: SqlUnitOfWork(sessions)
            self.broker = make_broker(self.config.rabbitmq_url)
            self.stack.push_async_callback(self.broker.stop)
            # API remains available while broker is down: publisher reconnects independently.
            self.publisher_task = asyncio.create_task(self._publish_loop(), name="outbox-publisher")
            self.stack.push_async_callback(self._stop_publisher)
            return self
        except BaseException:
            await self.stack.aclose()
            raise

    async def _publish_loop(self) -> None:
        publisher = RabbitEventPublisher(self.broker, timeout=self.config.publish_timeout)
        publish = PublishOutboxBatch(
            self.uow_factory,
            publisher,
            batch_size=self.config.outbox_batch_size,
        )
        while True:
            try:
                async with asyncio.timeout(self.config.publish_timeout):
                    await self.broker.connect()
                    await declare_topology(self.broker)
                await publish.execute()
            except Exception:
                logger.exception("Outbox publication failed; retained for retry")
            await asyncio.sleep(self.config.outbox_poll_seconds)

    async def _stop_publisher(self) -> None:
        self.publisher_task.cancel()
        try:
            await self.publisher_task
        except asyncio.CancelledError:
            pass

    async def __aexit__(self, *args) -> None:
        await self.stack.__aexit__(*args)


async def run_worker(config: Settings, *, stop_event: asyncio.Event | None = None) -> None:
    async with AsyncExitStack() as stack:
        engine = create_async_engine(config.database_url, pool_pre_ping=True)
        stack.push_async_callback(engine.dispose)
        sessions = async_sessionmaker(engine, expire_on_commit=False)

        def factory():
            return SqlUnitOfWork(sessions)

        client = await stack.enter_async_context(
            httpx.AsyncClient(
                timeout=config.webhook_timeout,
                follow_redirects=False,
                trust_env=False,
            )
        )
        broker = make_broker(config.rabbitmq_url)
        stack.push_async_callback(broker.stop)
        process = ProcessPayment(
            factory,
            SimulatedGateway(),
            HttpWebhookSender(client),
            retry_base_seconds=config.retry_base_seconds,
        )
        register_consumer(broker, ConsumerHandler(process, factory))
        await broker.connect()
        await declare_topology(broker)
        await broker.start()
        await (stop_event or asyncio.Event()).wait()
