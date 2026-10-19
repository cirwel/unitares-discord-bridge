"""SQLite state cache for event cursors, channel mappings, and HUD state."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from collections import OrderedDict

import aiosqlite

log = logging.getLogger(__name__)


class FindingDeliveryCache:
    """Coalesce exact REST/WS copies after confirmed delivery, per channel.

    Findings reach the bridge twice by design: governance broadcasts each one
    on the /ws/eisv firehose AND serves it on /api/events, and both senders
    stay live as fallbacks for each other. This cache is what makes that one
    post. Not incident suppression: distinct findings still deliver, and a
    failed send leaves the other sender free to try.

    The in-memory map is bounded. With a ``store`` (the bridge's SQLite
    cache) confirmed deliveries also survive a restart: the REST cursor is
    persisted, so after a restart the poller re-reads events the WS path had
    already posted in the last poll interval, and a process-local map would
    post them again.
    """

    def __init__(self, max_entries: int = 1000, store: "BridgeCache | None" = None) -> None:
        self._sent: OrderedDict[str, int] = OrderedDict()
        self._pending: dict[str, asyncio.Future] = {}
        self._max_entries = max_entries
        self._store = store

    async def _stored(self, key: str) -> int | None:
        if self._store is None:
            return None
        try:
            return await self._store.get_finding_delivery(key)
        except Exception as exc:  # noqa: BLE001 - persistence is best-effort
            log.debug("finding delivery lookup failed: %s", exc)
            return None

    async def _remember(self, key: str, message_id: int) -> None:
        self._sent[key] = message_id
        while len(self._sent) > self._max_entries:
            self._sent.popitem(last=False)
        if self._store is None:
            return
        try:
            await self._store.put_finding_delivery(key, message_id)
        except Exception as exc:  # noqa: BLE001 - persistence is best-effort
            log.debug("finding delivery record failed: %s", exc)

    async def send(self, channel, embed, event: dict | None, **send_kwargs) -> tuple[int | None, bool]:
        event = event or {}
        channel_id = getattr(channel, "id", None)
        eligible = (
            str(event.get("type") or "").endswith("_finding")
            and event.get("event_id") is not None
            and event.get("timestamp")
            and isinstance(channel_id, int)
        )
        if not eligible:
            message = await channel.send(embed=embed, **send_kwargs)
            return getattr(message, "id", None), False
        identity = [channel_id] + [event.get(k) for k in (
            "type", "event_id", "timestamp", "agent_id", "severity",
            "fingerprint", "message", "tags", "evidence",
        )]
        key = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
        # Only identical sends wait on each other; an unrelated critical alert
        # must not wait behind another channel's network operation.
        while key in self._pending:
            await asyncio.shield(self._pending[key])
        if key in self._sent:
            self._sent.move_to_end(key)
            return self._sent[key], True
        pending = asyncio.get_running_loop().create_future()
        self._pending[key] = pending
        try:
            stored = await self._stored(key)
            if stored is not None:
                self._sent[key] = stored
                return stored, True
            message = await channel.send(embed=embed, **send_kwargs)
            message_id = getattr(message, "id", None)
            if isinstance(message_id, int):
                await self._remember(key, message_id)
            return message_id, False
        finally:
            self._pending.pop(key, None)
            pending.set_result(None)


class BridgeCache:
    """Async context manager wrapping an aiosqlite database for bridge state."""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._db: aiosqlite.Connection | None = None

    async def __aenter__(self) -> "BridgeCache":
        self._db = await aiosqlite.connect(self._db_path)
        await self._create_tables()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._db is not None:
            await self._db.close()
            self._db = None

    async def _create_tables(self) -> None:
        assert self._db is not None
        await self._db.executescript(
            """
            CREATE TABLE IF NOT EXISTS kv (
                key   TEXT PRIMARY KEY,
                value TEXT
            );
            CREATE TABLE IF NOT EXISTS finding_deliveries (
                key          TEXT PRIMARY KEY,
                message_id   INTEGER NOT NULL,
                delivered_at REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_finding_deliveries_at
                ON finding_deliveries (delivered_at);
            """
        )

    # -- confirmed finding deliveries ---------------------------------------

    # Long enough to cover any REST replay after a restart or governance
    # outage; short enough that the table stays small (tens of rows a day).
    FINDING_DELIVERY_RETENTION_SECONDS = 7 * 24 * 3600

    async def get_finding_delivery(self, key: str) -> int | None:
        """Discord message id a finding identity was already delivered as."""
        assert self._db is not None
        async with self._db.execute(
            "SELECT message_id FROM finding_deliveries WHERE key = ?", (key,)
        ) as cur:
            row = await cur.fetchone()
        return int(row[0]) if row else None

    async def put_finding_delivery(self, key: str, message_id: int) -> None:
        """Record a confirmed delivery and drop rows past retention."""
        assert self._db is not None
        now = time.time()
        await self._db.execute(
            "INSERT INTO finding_deliveries (key, message_id, delivered_at)"
            " VALUES (?, ?, ?)"
            " ON CONFLICT(key) DO UPDATE SET message_id = excluded.message_id,"
            " delivered_at = excluded.delivered_at",
            (key, int(message_id), now),
        )
        await self._db.execute(
            "DELETE FROM finding_deliveries WHERE delivered_at < ?",
            (now - self.FINDING_DELIVERY_RETENTION_SECONDS,),
        )
        await self._db.commit()

    # -- event cursor --------------------------------------------------------

    async def get_event_cursor(self) -> int:
        """Return the last-seen event cursor, defaulting to 0.

        A prior bug wrote non-integer event_ids (e.g. UUIDs from a transient
        governance schema) into the cursor. Defensively parse: on any failure
        we log and return 0, which re-replays recent events once — preferable
        to every poll raising.
        """
        assert self._db is not None
        async with self._db.execute(
            "SELECT value FROM kv WHERE key = 'event_cursor'"
        ) as cur:
            row = await cur.fetchone()
        if not row:
            return 0
        try:
            return int(row[0])
        except (ValueError, TypeError):
            log.warning("Corrupt event_cursor %r; resetting to 0", row[0])
            return 0

    async def set_event_cursor(self, cursor: int) -> None:
        """Upsert the event cursor."""
        assert self._db is not None
        await self._db.execute(
            "INSERT INTO kv (key, value) VALUES ('event_cursor', ?)"
            " ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (str(cursor),),
        )
        await self._db.commit()

    # -- generic key/value ---------------------------------------------------

    async def get_kv(self, key: str) -> str | None:
        """Return the stored string for ``key``, or None if absent."""
        assert self._db is not None
        async with self._db.execute(
            "SELECT value FROM kv WHERE key = ?", (key,)
        ) as cur:
            row = await cur.fetchone()
        return row[0] if row else None

    async def set_kv(self, key: str, value: str) -> None:
        """Upsert a string value under ``key``."""
        assert self._db is not None
        await self._db.execute(
            "INSERT INTO kv (key, value) VALUES (?, ?)"
            " ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )
        await self._db.commit()

    # -- HUD message ---------------------------------------------------------

    async def get_hud_message(self) -> tuple[int, int] | None:
        """Return (channel_id, message_id) for the HUD message, or None."""
        assert self._db is not None
        async with self._db.execute(
            "SELECT value FROM kv WHERE key = 'hud_message'"
        ) as cur:
            row = await cur.fetchone()
        if row is None:
            return None
        data = json.loads(row[0])
        return (data["channel_id"], data["message_id"])

    async def set_hud_message(self, channel_id: int, message_id: int) -> None:
        """Store the HUD message location."""
        assert self._db is not None
        value = json.dumps({"channel_id": channel_id, "message_id": message_id})
        await self._db.execute(
            "INSERT INTO kv (key, value) VALUES ('hud_message', ?)"
            " ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (value,),
        )
        await self._db.commit()
