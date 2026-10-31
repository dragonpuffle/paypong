import asyncio
import logging
import signal

from payment_service.bootstrap import run_worker
from payment_service.config import Settings


async def serve() -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    installed = []
    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop.set)
                installed.append(sig)
            except NotImplementedError:
                pass  # Windows: asyncio.run still handles Ctrl+C cancellation.
        await run_worker(Settings(), stop_event=stop)
    finally:
        for sig in installed:
            loop.remove_signal_handler(sig)


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(serve())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
