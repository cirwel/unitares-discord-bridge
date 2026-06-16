"""SQLite state cache for event cursors, channel mappings, and HUD state."""

from __future__ import annotations

import json
import logging

import aiosqlite

log = logging.getLogger(__name__)


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
            """
        )

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
