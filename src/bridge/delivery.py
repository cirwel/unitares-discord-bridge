"""Delivery to Discord through transient connection failures.

Between 2026-10-05 and 2026-10-19 the bridge lost 65 posts to
``Cannot connect to host discord.com:443 ... nodename nor servname provided``:
short local DNS blips that discord.py does not retry. The send loops treated
the error as unexpected, recorded one ``bridge.delivery_failed`` receipt and
dropped the message, including Lumen silent-critical alerts.

Two pieces:

* :class:`DiscordSender` is the queue + send loop shared by the REST poller
  and the WebSocket subscriber.

  - Every queued message carries a stable ``nonce``. discord.py sends it with
    ``enforce_nonce``, so if a send that failed with a reset or timeout had
    in fact been written, the retry returns the existing message instead of
    posting a second copy.
  - Circuit breaker: the first transient connection failure opens it. While
    it is open nothing is sent and nobody sleeps per message; the failed
    message goes straight back on the queue. When the probe delay (doubling
    from ``SEND_RETRY_BACKOFF_SECONDS`` up to ``SEND_PROBE_MAX_SECONDS``)
    elapses, the next message in line is the probe. One success closes it.
  - #alerts messages jump the queue (:class:`SendQueue`), so a critical item
    enqueued during an outage is the first thing delivered on recovery.
  - A message the sender took and could not deliver goes back to the front of
    its lane regardless of ``maxsize``; an #alerts message put into a full
    queue evicts the oldest routine one (whose loss is receipted) rather than
    failing. A message still undeliverable after ``SEND_MAX_AGE_SECONDS`` is
    recorded durably as ``bridge.delivery_failed`` with reason
    ``discord_unreachable`` and dropped.
  - The loop supervises itself: an error escaping per-message handling is
    logged at error level, counted in ``loop_restarts``, and the loop keeps
    running. Rate-limit handling is unchanged from the original loops.

* :func:`send_with_retry` for the few direct, unqueued sends (Lumen
  offline/online notices, self-iteration posts). Those have no nonce, so
  they retry only failures that happen before any request is written
  (connection could not be established: DNS, refused). A reset or timeout
  after the body went out is not retried there, because it could duplicate.
"""

from __future__ import annotations

import asyncio
import collections
import hashlib
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, TypeVar

import aiohttp
import discord

from bridge import config

log = logging.getLogger(__name__)

T = TypeVar("T")

#: Discord could not be reached at all, or the connection broke mid-request.
#: ``ClientConnectorError`` (incl. ``ClientConnectorDNSError``) and
#: ``ServerDisconnectedError`` are ``ClientConnectionError`` subclasses;
#: ``TimeoutError``/``asyncio.TimeoutError`` cover a hung request. Safe to
#: retry only with a nonce: the request may have been written.
TRANSIENT_SEND_ERRORS: tuple[type[BaseException], ...] = (
    aiohttp.ClientConnectionError,
    asyncio.TimeoutError,
    OSError,
)

#: Failures raised before any request was written (the TCP/TLS connection
#: was never established), safe to retry without a nonce.
PRE_SEND_ERRORS: tuple[type[BaseException], ...] = (aiohttp.ClientConnectorError,)

#: Discord's nonce limit.
_NONCE_LEN = 25


def is_transient_send_error(exc: BaseException) -> bool:
    """True when ``exc`` means Discord was unreachable, not that it refused."""
    if isinstance(exc, discord.HTTPException):
        # Discord answered. Rate limits and 4xx/5xx have their own handling.
        return False
    return isinstance(exc, TRANSIENT_SEND_ERRORS)


def is_pre_send_error(exc: BaseException) -> bool:
    """True when the request certainly never reached Discord."""
    return not isinstance(exc, discord.HTTPException) and isinstance(exc, PRE_SEND_ERRORS)


async def send_with_retry(
    send: Callable[[], Awaitable[T]],
    *,
    what: str = "discord send",
) -> T:
    """Await ``send()``, retrying pre-send connection failures with backoff.

    For direct sends without a nonce. ``send`` is a zero-argument callable so
    each attempt makes a fresh request. Anything else (including a reset or
    timeout after the request may have been written) propagates immediately;
    the last pre-send error propagates once ``config.SEND_ATTEMPTS`` are used.
    """
    attempts = max(1, int(config.SEND_ATTEMPTS))
    for attempt in range(1, attempts + 1):
        try:
            return await send()
        except Exception as exc:
            if not is_pre_send_error(exc) or attempt >= attempts:
                raise
            delay = config.SEND_RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1))
            log.warning(
                "%s: Discord unreachable (%s); attempt %d/%d, retrying in %.1fs",
                what, exc, attempt, attempts, delay,
            )
            await asyncio.sleep(delay)
    raise AssertionError("unreachable")  # pragma: no cover


# ---------------------------------------------------------------------------
# Queued delivery
# ---------------------------------------------------------------------------


def message_nonce(channel: Any, source_event: dict | None) -> str:
    """A nonce stable across retries of one logical message.

    Derived from the destination and the source event's identity when there
    is one (so the REST and WS copies of a finding share it too); otherwise
    random, fixed once per queued message.
    """
    event = source_event or {}
    if event.get("type") and event.get("event_id") is not None:
        identity = [
            getattr(channel, "id", None),
            *(event.get(k) for k in ("type", "event_id", "timestamp", "agent_id", "fingerprint")),
        ]
        raw = json.dumps(identity, sort_keys=True, default=str).encode()
        return hashlib.sha256(raw).hexdigest()[:_NONCE_LEN]
    return uuid.uuid4().hex[:_NONCE_LEN]


@dataclass
class Outgoing:
    """One queued Discord message and its delivery state."""

    channel: Any
    embed: discord.Embed
    source_event: dict | None = None
    attempts: int = 0
    nonce: str = ""
    enqueued_at: float = field(default_factory=time.monotonic)

    def __post_init__(self) -> None:
        if not self.nonce:
            self.nonce = message_nonce(self.channel, self.source_event)


def as_outgoing(item: Any) -> Outgoing:
    """Accept an :class:`Outgoing` or a legacy ``(channel, embed, source_event
    [, attempts])`` tuple. Raises ``ValueError`` on anything else."""
    if isinstance(item, Outgoing):
        return item
    if isinstance(item, tuple) and len(item) in (3, 4):
        channel, embed, source_event, *rest = item
        attempts = rest[0] if rest and isinstance(rest[0], int) else 0
        return Outgoing(channel, embed, source_event, attempts=attempts)
    raise ValueError(f"malformed send-queue item: {type(item).__name__}")


def _item_channel(item: Any) -> Any:
    if isinstance(item, Outgoing):
        return item.channel
    if isinstance(item, tuple) and item:
        return item[0]
    return None


class SendQueue(asyncio.Queue):
    """``asyncio.Queue`` with an express lane: items for which ``is_priority``
    is true (messages to #alerts) are always dequeued first, FIFO within each
    lane. Items are returned exactly as put.

    Two rules keep an alert from being the thing that gets dropped:

    * :meth:`requeue` puts a message the sender already took back at the
      FRONT of its own lane and ignores ``maxsize``. While a send is awaited
      a blocked producer can refill the freed slot; a failed alert must not
      then find the queue full.
    * An urgent item put into a full queue evicts the oldest ROUTINE item
      instead of failing. ``on_evict(item)`` is called with the evicted item
      so its loss can be recorded. Only a queue full of urgent items rejects
      an urgent put.
    """

    def __init__(
        self,
        maxsize: int = 0,
        *,
        is_priority: Callable[[Any], bool] | None = None,
        on_evict: Callable[[Any], None] | None = None,
    ) -> None:
        self._is_priority = is_priority or (lambda _item: False)
        self._on_evict = on_evict
        super().__init__(maxsize)

    def _urgent_item(self, item: Any) -> bool:
        try:
            return bool(self._is_priority(item))
        except Exception:  # noqa: BLE001 - a bad item must not break the queue
            return False

    def _make_room_for(self, item: Any) -> None:
        """Evict the oldest routine item if ``item`` is urgent and we're full."""
        if not (self.full() and self._queue and self._urgent_item(item)):
            return
        evicted = self._queue.popleft()
        self.task_done()  # the evicted item will never be processed
        if self._on_evict is not None:
            try:
                self._on_evict(evicted)
            except Exception:  # noqa: BLE001 - recording a loss must not lose the alert
                log.exception("send queue eviction hook failed")

    def put_nowait(self, item: Any) -> None:
        self._make_room_for(item)
        super().put_nowait(item)

    async def put(self, item: Any) -> None:
        self._make_room_for(item)
        if self._urgent_item(item) and not self.full():
            super().put_nowait(item)
            return
        await super().put(item)

    def requeue(self, item: Any) -> None:
        """Return a taken item to the front of its lane, ignoring ``maxsize``."""
        (self._urgent if self._urgent_item(item) else self._queue).appendleft(item)
        # Same bookkeeping as asyncio.Queue.put_nowait (stable across 3.12-3.14).
        self._unfinished_tasks += 1
        self._finished.clear()
        self._wakeup_next(self._getters)

    def _init(self, maxsize: int) -> None:  # noqa: D401 - asyncio.Queue hook
        self._queue = collections.deque()
        self._urgent: collections.deque = collections.deque()

    # asyncio.Queue's qsize()/empty() read ``self._queue`` directly (and
    # full()/get()/put() build on them), which would ignore the express lane.
    def qsize(self) -> int:
        return len(self._queue) + len(self._urgent)

    def empty(self) -> bool:
        return self.qsize() == 0

    def _put(self, item: Any) -> None:
        (self._urgent if self._urgent_item(item) else self._queue).append(item)

    def _get(self) -> Any:
        return self._urgent.popleft() if self._urgent else self._queue.popleft()


ReceiptFn = Callable[..., Awaitable[None]]


class DiscordSender:
    """Queue + supervised send loop shared by the REST poller and WS subscriber."""

    def __init__(
        self,
        *,
        name: str,
        record_receipt: ReceiptFn,
        maxsize: int,
        priority_channel: Callable[[], Any] = lambda: None,
        delivery_cache: Any = None,
        pacing: float = 0.15,
    ) -> None:
        self.name = name
        self._record_receipt = record_receipt
        self._priority_channel = priority_channel
        self.delivery_cache = delivery_cache
        self.pacing = pacing
        self.queue = SendQueue(maxsize, is_priority=self._is_priority, on_evict=self._evicted)
        self._eviction_tasks: set[asyncio.Task] = set()
        # Circuit breaker: monotonic time before which nothing is sent.
        self._outage_until = 0.0
        self._probe_delay = 0.0
        self.loop_restarts = 0
        self.restart_delay = 1.0

    # -- queue helpers -----------------------------------------------------

    def _is_priority(self, item: Any) -> bool:
        target = self._priority_channel()
        return target is not None and _item_channel(item) is target

    @property
    def in_outage(self) -> bool:
        return time.monotonic() < self._outage_until

    def _requeue(self, item: Outgoing) -> None:
        """Back to the front of its lane; never dropped for lack of room."""
        requeue = getattr(self.queue, "requeue", None)
        if requeue is not None:
            requeue(item)
        else:  # a plain asyncio.Queue swapped in by a caller
            self.queue.put_nowait(item)

    def _evicted(self, raw: Any) -> None:
        """An alert displaced this routine message from a full queue."""
        try:
            item = as_outgoing(raw)
        except ValueError:
            return
        log.warning("%s: send queue full; dropped a routine message for an alert", self.name)
        try:
            task = asyncio.get_running_loop().create_task(
                self._record_receipt(
                    "bridge.delivery_failed",
                    source_event=item.source_event,
                    channel=item.channel,
                    reason="send_queue_full",
                    error="evicted for an #alerts message",
                )
            )
        except RuntimeError:  # no running loop (sync test context)
            return
        self._eviction_tasks.add(task)
        task.add_done_callback(self._eviction_tasks.discard)

    # -- breaker -----------------------------------------------------------

    def _open_breaker(self, exc: BaseException) -> None:
        base = max(0.0, float(config.SEND_RETRY_BACKOFF_SECONDS))
        cap = max(base, float(config.SEND_PROBE_MAX_SECONDS))
        self._probe_delay = base if self._probe_delay <= 0 else min(cap, self._probe_delay * 2)
        self._outage_until = time.monotonic() + self._probe_delay
        log.warning(
            "%s: Discord unreachable (%s); pausing sends, next probe in %.1fs (queued: %d)",
            self.name, exc, self._probe_delay, self.queue.qsize(),
        )

    def _close_breaker(self) -> None:
        if self._probe_delay > 0:
            log.info("%s: Discord reachable again; resuming sends", self.name)
        self._outage_until = 0.0
        self._probe_delay = 0.0

    # -- sending -----------------------------------------------------------

    async def _send(self, item: Outgoing) -> tuple[int | None, bool]:
        if self.delivery_cache is not None:
            return await self.delivery_cache.send(
                item.channel, item.embed, item.source_event, nonce=item.nonce,
            )
        message = await item.channel.send(embed=item.embed, nonce=item.nonce)
        return getattr(message, "id", None), False

    async def _handle_unreachable(self, item: Outgoing, exc: BaseException) -> None:
        self._open_breaker(exc)
        age = time.monotonic() - item.enqueued_at
        if age >= float(config.SEND_MAX_AGE_SECONDS):
            log.error(
                "%s: Discord unreachable for %.0fs; giving up on message (%s)",
                self.name, age, exc,
            )
            await self._record_receipt(
                "bridge.delivery_failed",
                source_event=item.source_event,
                channel=item.channel,
                reason="discord_unreachable",
                error=str(exc),
            )
            return
        # Straight back on the queue: no per-message sleep during an outage.
        self._requeue(item)

    async def _process(self, item: Outgoing) -> None:
        item.attempts += 1
        try:
            message_id, duplicate = await self._send(item)
            self._close_breaker()
            await self._record_receipt(
                "bridge.suppressed" if duplicate else "bridge.delivery",
                source_event=item.source_event,
                channel=item.channel,
                discord_message_id=message_id,
                reason="duplicate_finding_delivery" if duplicate else None,
            )
        except discord.RateLimited as exc:
            # Raised when max_ratelimit_timeout is set and retry-after exceeds
            # it. Respect Discord's back-off and re-queue, never drop.
            self._close_breaker()
            log.warning("%s: rate limited; retrying in %.1fs", self.name, exc.retry_after)
            await self._record_receipt(
                "bridge.rate_limited",
                source_event=item.source_event,
                channel=item.channel,
                reason="discord_rate_limited",
            )
            await asyncio.sleep(exc.retry_after)
            self._requeue(item)
        except discord.HTTPException as exc:
            self._close_breaker()
            if exc.status == 429:
                retry = float(exc.response.headers.get("Retry-After", 5))
                log.warning("%s: rate limited (HTTP 429); retrying in %.1fs", self.name, retry)
                await self._record_receipt(
                    "bridge.rate_limited",
                    source_event=item.source_event,
                    channel=item.channel,
                    reason="discord_http_429",
                )
                await asyncio.sleep(retry)
                self._requeue(item)
            else:
                log.warning("%s: discord send failed (%s)", self.name, exc)
                await self._record_receipt(
                    "bridge.delivery_failed",
                    source_event=item.source_event,
                    channel=item.channel,
                    reason="discord_http_exception",
                    error=str(exc),
                )
        except Exception as exc:
            if is_transient_send_error(exc):
                await self._handle_unreachable(item, exc)
                return
            # Malformed embed state etc.: drop this message, keep draining.
            log.error(
                "%s: unexpected send error (%s); dropping message", self.name, exc,
                exc_info=exc,
            )
            await self._record_receipt(
                "bridge.delivery_failed",
                source_event=item.source_event,
                channel=item.channel,
                reason="send_loop_unexpected_error",
                error=str(exc),
            )

    async def _run_once(self) -> None:
        # Breaker open: wait out the probe delay without touching the queue.
        # The next item taken afterwards is the probe, #alerts first.
        remaining = self._outage_until - time.monotonic()
        if remaining > 0:
            await asyncio.sleep(remaining)
            return
        raw = await self.queue.get()
        try:
            item = as_outgoing(raw)
        except ValueError as exc:
            log.error("%s: %s; dropped", self.name, exc)
            return
        await self._process(item)
        # Pacing between sends to stay under Discord's per-route burst limits.
        await asyncio.sleep(self.pacing)

    async def run(self) -> None:
        """Drain the queue forever. Supervised: it does not die silently."""
        while True:
            try:
                await self._run_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - supervisor
                self.loop_restarts += 1
                log.error(
                    "%s: send loop error (restart #%d): %s",
                    self.name, self.loop_restarts, exc, exc_info=exc,
                )
                await asyncio.sleep(self.restart_delay)
