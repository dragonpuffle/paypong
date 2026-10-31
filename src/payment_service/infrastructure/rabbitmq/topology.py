from faststream.rabbit import Channel, RabbitBroker, RabbitQueue

PAYMENTS = RabbitQueue(
    "payments.new",
    durable=True,
    arguments={"x-single-active-consumer": True},
)
DLQ = RabbitQueue("payments.dlq", durable=True)


def make_broker(url: str) -> RabbitBroker:
    return RabbitBroker(
        url,
        timeout=5,
        graceful_timeout=30,
        default_channel=Channel(
            prefetch_count=1,
            publisher_confirms=True,
            on_return_raises=True,
        ),
    )


async def declare_topology(broker: RabbitBroker) -> None:
    await broker.declare_queue(PAYMENTS)
    await broker.declare_queue(DLQ)
