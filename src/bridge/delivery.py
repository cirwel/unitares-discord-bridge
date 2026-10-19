"""Bounded retry for transient Discord connection failures.

Between 2026-10-05 and 2026-10-19 the bridge lost 65 posts to
``Cannot connect to host discord.com:443 ... nodename nor servname provided``:
short local DNS blips that discord.py does not retry (its HTTP client retries
5xx responses and connection resets, not a failed name lookup). The send loops
treated the error as unexpected, recorded one ``bridge.delivery_failed``
receipt and dropped the message — including Lumen silent-critical alerts.

Two layers, deliberately small:

1. :func:`send_with_retry` retries a single send a few times with a short
   backoff, which covers a blip of a few seconds without reordering anything.
2. The send loops re-queue a message that still could not be delivered, a
   bounded number of times, spaced by :data:`config.SEND_REDELIVERY_DELAY_SECONDS`
   so an outage of a few minutes is ridden out without blocking the queue.
   Only when that is exhausted (or the queue is full) is the loss recorded,
   durably, as ``bridge.delivery_failed`` with reason ``discord_unreachable``.

Rate limits keep their existing handling in the send loops; HTTP errors that
Discord actually answered (4xx/5xx) are not transient connection failures and
are never retried here.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable, TypeVar

import aiohttp
import discord

from bridge import config

log = logging.getLogger(__name__)

T = TypeVar("T")

#: Errors raised when Discord could not be reached at all. ``ClientConnectorError``
#: (the DNS failure above) and ``ClientOSError`` are ``ClientConnectionError``
#: subclasses; ``TimeoutError``/``asyncio.TimeoutError`` cover a hung connect.
TRANSIENT_SEND_ERRORS: tuple[type[BaseException], ...] = (
    aiohttp.ClientConnectionError,
    asyncio.TimeoutError,
    OSError,
)


def is_transient_send_error(exc: BaseException) -> bool:
    """True when ``exc`` means Discord was unreachable, not that it refused."""
    if isinstance(exc, discord.HTTPException):
        # Discord answered. Rate limits and 4xx/5xx have their own handling.
        return False
    return isinstance(exc, TRANSIENT_SEND_ERRORS)


async def send_with_retry(
    send: Callable[[], Awaitable[T]],
    *,
    what: str = "discord send",
) -> T:
    """Await ``send()``, retrying transient connection errors with backoff.

    ``send`` is a zero-argument callable so each attempt makes a fresh request.
    Non-transient exceptions propagate immediately; the last transient error
    propagates once :data:`config.SEND_ATTEMPTS` attempts are used up.
    """
    attempts = max(1, int(config.SEND_ATTEMPTS))
    for attempt in range(1, attempts + 1):
        try:
            return await send()
        except Exception as exc:
            if not is_transient_send_error(exc) or attempt >= attempts:
                raise
            delay = config.SEND_RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1))
            log.warning(
                "%s: Discord unreachable (%s); attempt %d/%d, retrying in %.1fs",
                what, exc, attempt, attempts, delay,
            )
            await asyncio.sleep(delay)
    raise AssertionError("unreachable")  # pragma: no cover
