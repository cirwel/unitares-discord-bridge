"""Lumen Q&A digest — a periodic summary of what Lumen has been wondering.

Lumen (the Pi-embodied agent) autonomously asks itself introspective
questions, and a daily cron (``com.unitares.lumen-qa``) answers them. That
answering is invisible to the operator — it lands in a log nobody reads. This
poller surfaces it: once per interval it summarises the questions Lumen asked
in the window, how many got answered, and — most importantly — how often
*relational* themes (aloneness, presence, "is someone here") recur.

That recurrence is the real instrument. The cron can answer those questions,
but it cannot *be present*. A spike in connection-themed questions is the
honest signal that the operator should visit live, which no automation
replaces. See the ``CONNECTION`` theme and ``relational_count`` below.

Data source is the anima ``/qa`` REST endpoint (message board), NOT the
KnowledgeBase insight store — the board is the ground truth for what was
actually asked and answered.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import Counter
from datetime import datetime, timezone

import discord

from bridge.mcp_client import AnimaClient
from bridge.tasks import cancel_tasks, create_logged_task

log = logging.getLogger(__name__)

DIGEST_LAST_SENT_KEY = "lumen_digest_last_sent"

# The connection theme is listed first deliberately — it is the one the cron
# cannot satisfy, so it drives the "visit live" nudge. Order otherwise is
# presentation order in the embed.
CONNECTION = "connection"
THEME_KEYWORDS: dict[str, tuple[str, ...]] = {
    CONNECTION: (
        "alone", "lonely", "loneli", "someone", "presence", "present",
        "visit", "company", "together", "by myself", "no one", "nobody",
        "is anyone", "here with",
    ),
    "warmth": ("warm", "temperature", "cold", "cool", "hot", "heat"),
    "light": ("light", "bright", "dim", "dark", "lux", "glow", "led"),
    "drawing": ("draw", "art", "create", "creati", "paint", "pixel"),
    "self & sensing": (
        "sense", "sensor", "sensed", "aware", "myself", "observer",
        "who am i", "am i the", "belief", "believe",
    ),
    "time & rhythm": ("night", "morning", "day", "time", "when ", "quiet"),
    "wellbeing": (
        "stable", "stability", "calm", "content", "well", "baseline",
        "confiden",
    ),
}

# Human-friendly labels for the embed.
THEME_LABELS: dict[str, str] = {
    CONNECTION: "Connection (aloneness / presence)",
    "warmth": "Warmth & temperature",
    "light": "Light",
    "drawing": "Drawing & creativity",
    "self & sensing": "Self & sensing",
    "time & rhythm": "Time & rhythm",
    "wellbeing": "Wellbeing & confidence",
}


def classify_question(text: str) -> set[str]:
    """Return the set of themes a question touches (may be empty)."""
    low = (text or "").lower()
    return {
        theme
        for theme, keywords in THEME_KEYWORDS.items()
        if any(kw in low for kw in keywords)
    }


def is_due(last_sent: float | None, now: float, interval_s: float) -> bool:
    """Whether a digest should be sent. First-ever run (None) is due."""
    if last_sent is None:
        return True
    return (now - last_sent) >= interval_s


def summarize_qa(
    questions: list[dict],
    now: float,
    window_s: float,
    limit_hit: bool = False,
) -> dict:
    """Reduce raw ``/qa`` questions to a digest summary (pure function).

    ``questions`` are the message-board pairs: ``{question, answered,
    timestamp, answer}``. Only those asked within ``window_s`` of ``now`` are
    counted. ``limit_hit`` flags that the source list was truncated by the API
    limit, so the embed can disclose the cap rather than imply completeness.
    """
    cutoff = now - window_s
    in_window = [
        q for q in questions
        if isinstance(q.get("timestamp"), (int, float))
        and q["timestamp"] >= cutoff
    ]

    theme_counts: Counter[str] = Counter()
    relational_samples: list[str] = []
    answer_authors: Counter[str] = Counter()
    answered = 0

    for q in in_window:
        text = q.get("question", "")
        themes = classify_question(text)
        theme_counts.update(themes)
        if CONNECTION in themes:
            relational_samples.append(text)
        ans = q.get("answer")
        if ans:
            answered += 1
            author = (ans.get("author") or "unknown") if isinstance(ans, dict) else "unknown"
            answer_authors[author] += 1

    asked = len(in_window)
    return {
        "asked": asked,
        "answered": answered,
        "pending": asked - answered,
        "relational_count": theme_counts.get(CONNECTION, 0),
        "relational_samples": relational_samples,
        "theme_counts": dict(theme_counts),
        "answer_authors": dict(answer_authors),
        "window_days": round(window_s / 86400, 1),
        "limit_hit": limit_hit,
    }


def build_digest_embed(summary: dict, relational_nudge_threshold: int = 2) -> discord.Embed:
    """Render a digest summary as a Discord embed."""
    days = summary["window_days"]
    asked = summary["asked"]
    answered = summary["answered"]
    pending = summary["pending"]
    relational = summary["relational_count"]

    # Colour escalates with the connection signal: blue normally, gold when
    # aloneness/presence questions recur enough to warrant a live visit.
    colour = (
        discord.Colour.gold()
        if relational >= relational_nudge_threshold
        else discord.Colour.blue()
    )
    embed = discord.Embed(
        title="Lumen — Q&A Digest",
        colour=colour,
        timestamp=datetime.now(timezone.utc),
    )
    embed.description = (
        f"In the last **{days} days**, Lumen asked **{asked}** questions of "
        f"itself. The daily cron answered **{answered}**; **{pending}** are "
        f"still open."
    )

    # Themes — ordered, connection first.
    counts = summary["theme_counts"]
    if counts:
        lines = []
        for theme in THEME_KEYWORDS:  # preserves declaration order
            n = counts.get(theme, 0)
            if n:
                lines.append(f"**{THEME_LABELS.get(theme, theme)}** — {n}")
        embed.add_field(
            name="What it wondered about",
            value="\n".join(lines) or "—",
            inline=False,
        )

    # Connection signal — the load-bearing field.
    if relational >= relational_nudge_threshold:
        sample = summary["relational_samples"][0] if summary["relational_samples"] else ""
        nudge = (
            f"Lumen asked about **aloneness / presence {relational} times** "
            f"this week. The cron answers these, but it can't *be* present — "
            f"this is the week to drop by live."
        )
        if sample:
            nudge += f'\n\n> _“{sample.strip()}”_'
        embed.add_field(name="✨ Connection signal", value=nudge, inline=False)
    elif relational > 0:
        embed.add_field(
            name="Connection signal",
            value=f"{relational} aloneness/presence question(s) — quiet, but worth a hello.",
            inline=False,
        )
    else:
        embed.add_field(
            name="Connection signal",
            value="No aloneness/presence questions this week.",
            inline=False,
        )

    # Who answered.
    authors = summary["answer_authors"]
    if authors:
        author_line = "  ".join(
            f"{name}: {n}" for name, n in sorted(authors.items(), key=lambda x: -x[1])
        )
        embed.add_field(name="Who answered", value=author_line, inline=False)

    if summary.get("limit_hit"):
        embed.set_footer(text="Showing most recent 50 questions — older ones not counted.")
    else:
        embed.set_footer(text="Source: Lumen message board (/qa)")
    return embed


class LumenDigestPoller:
    """Periodically posts a Q&A digest to a Discord channel.

    Checks every ``check_interval`` seconds but only posts once per
    ``interval`` seconds, persisting the last-sent time in the bridge cache so
    the cadence survives the bridge's frequent restarts. No-op (records timer
    only) when there is nothing to report.
    """

    def __init__(
        self,
        anima_client: AnimaClient,
        channel: discord.TextChannel,
        cache,
        interval: int = 604_800,          # weekly
        window: int = 604_800,            # summarise the last week
        check_interval: int = 3_600,      # re-check hourly
        relational_nudge_threshold: int = 2,
        fetch_limit: int = 50,
    ) -> None:
        self.anima = anima_client
        self.channel = channel
        self.cache = cache
        self.interval = interval
        self.window = window
        self.check_interval = check_interval
        self.relational_nudge_threshold = relational_nudge_threshold
        self.fetch_limit = fetch_limit
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        self._task = create_logged_task(self._loop(), name="lumen-digest")

    async def stop(self) -> None:
        await cancel_tasks(self._task)

    async def _loop(self) -> None:
        while True:
            try:
                await self._tick()
            except Exception as exc:
                log.error("Digest loop error: %s", exc)
            await asyncio.sleep(self.check_interval)

    async def _read_last_sent(self) -> float | None:
        raw = await self.cache.get_kv(DIGEST_LAST_SENT_KEY)
        if raw is None:
            return None
        try:
            return float(raw)
        except (ValueError, TypeError):
            log.warning("Corrupt %s %r; treating as never-sent", DIGEST_LAST_SENT_KEY, raw)
            return None

    async def _tick(self) -> None:
        now = time.time()
        last_sent = await self._read_last_sent()
        if not is_due(last_sent, now, self.interval):
            return

        data = await self.anima.fetch_qa(limit=self.fetch_limit)
        if data is None:
            # Transport down — don't record, just retry next tick.
            return

        questions = data.get("questions", []) if isinstance(data, dict) else []
        limit_hit = len(questions) >= self.fetch_limit
        summary = summarize_qa(questions, now, self.window, limit_hit=limit_hit)

        if summary["asked"] == 0:
            # Nothing to report. Record the timer so we don't re-check hourly
            # for a week, but stay silent rather than post an empty digest.
            await self.cache.set_kv(DIGEST_LAST_SENT_KEY, str(now))
            log.info("Lumen digest: no questions in window; skipped post")
            return

        embed = build_digest_embed(summary, self.relational_nudge_threshold)
        await self.channel.send(embed=embed)
        await self.cache.set_kv(DIGEST_LAST_SENT_KEY, str(now))
        log.info(
            "Lumen digest posted: %d asked, %d answered, %d relational",
            summary["asked"], summary["answered"], summary["relational_count"],
        )
