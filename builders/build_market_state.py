#!/usr/bin/env python3
"""Builder 2 — état de marché au moment de décision de l'épisode.

Pour chaque série, on sélectionne une observation dont :

* la date de marché est strictement antérieure à `decision_at.date()` ;
* la révision a été collectée strictement avant `decision_at`.

Cette seconde condition est essentielle : prendre la dernière révision globale
de FRED réécrirait le passé. `coverage_ok` exige en plus des pivots SP500 et VIX
présents et non périmés. La table dérivée est reconstruite atomiquement.
"""
from __future__ import annotations

import bisect
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Iterable

import psycopg2
from psycopg2.extras import Json, execute_values

SERIES = ["SP500", "VIXCLS", "DCOILWTICO", "DTWEXBGS", "T10Y2Y", "DFF", "DHHNGSP"]
MAX_PIVOT_STALENESS_DAYS = 7
PIPELINE_LOCK = "market-memory-build-pipeline-v2"
REPRESENTATION_VERSION = "pit-v2-observed-window-6h"


def _aware_utc(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} doit être timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True)
class FredVintage:
    observed_on: date
    value: float
    fetched_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "fetched_at", _aware_utc(self.fetched_at, "fetched_at"))


def point_in_time(
    vintages: Iterable[FredVintage],
    decision_at: datetime,
    before_date: date | None = None,
) -> FredVintage | None:
    """Dernière date, puis dernière révision, toutes deux disponibles à la décision."""
    decision_at = _aware_utc(decision_at, "decision_at")
    date_limit = before_date or decision_at.date()
    eligible = [
        vintage for vintage in vintages
        if vintage.observed_on < date_limit and vintage.fetched_at < decision_at
    ]
    if not eligible:
        return None
    return max(eligible, key=lambda vintage: (vintage.observed_on, vintage.fetched_at))


def momentum(
    vintages: Iterable[FredVintage], decision_at: datetime, days: int = 5,
) -> float | None:
    current = point_in_time(vintages, decision_at)
    if current is None:
        return None
    previous = point_in_time(
        vintages,
        decision_at,
        before_date=decision_at.date() - timedelta(days=days),
    )
    if previous is None or previous.value == 0:
        return None
    return round((current.value / previous.value - 1) * 100, 2)


def trend_label(value: float | None) -> str | None:
    """Ne transforme jamais une donnée absente ou plate en faux signal baissier."""
    if value is None:
        return None
    if value > 0:
        return "up"
    if value < 0:
        return "down"
    return "flat"


def load_vintages(cur, series_id: str) -> list[FredVintage]:
    cur.execute(
        """SELECT date, value, fetched_at FROM fred_data
           WHERE series_id=%s AND value IS NOT NULL
             AND date IS NOT NULL AND fetched_at IS NOT NULL
           ORDER BY fetched_at, date""",
        (series_id,),
    )
    return [FredVintage(day, float(value), fetched_at) for day, value, fetched_at in cur.fetchall()]


def _dsn() -> str:
    value = os.getenv("EVENTS_DSN", "").strip()
    if not value:
        raise RuntimeError("EVENTS_DSN manque (rôle DB en écriture requis)")
    return value


def main() -> None:
    conn = psycopg2.connect(_dsn(), connect_timeout=15)
    conn.set_session(isolation_level="REPEATABLE READ")
    read_cur, write_cur = conn.cursor(), conn.cursor()
    read_cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", [PIPELINE_LOCK])

    series = {series_id: load_vintages(read_cur, series_id) for series_id in SERIES}

    read_cur.execute("""
        SELECT fetched_at, price
        FROM crypto_data
        WHERE symbol='BTC' AND price IS NOT NULL AND fetched_at IS NOT NULL
        ORDER BY fetched_at
    """)
    btc = read_cur.fetchall()
    btc_times = [_aware_utc(row[0], "crypto.fetched_at") for row in btc]
    btc_values = [float(row[1]) for row in btc]

    read_cur.execute("""
        SELECT me.id, me.t0, er.as_of
        FROM market_episodes me
        JOIN episode_representations er ON er.episode_id = me.id
        WHERE me.kind <> 'recurring' AND er.representation_version = %s
        ORDER BY er.as_of, me.id
    """, [REPRESENTATION_VERSION])
    episodes = read_cur.fetchall()
    batch: list[tuple] = []
    covered = 0
    for episode_id, t0, decision_at in episodes:
        decision_at = _aware_utc(decision_at, "decision_at")
        selected = {
            series_id: point_in_time(vintages, decision_at)
            for series_id, vintages in series.items()
        }
        pivots = (selected["SP500"], selected["VIXCLS"])
        coverage = all(
            pivot is not None
            and 0 <= (decision_at.date() - pivot.observed_on).days <= MAX_PIVOT_STALENESS_DAYS
            for pivot in pivots
        )

        state: dict = {}
        if coverage:
            for series_id, vintage in selected.items():
                if vintage is not None:
                    state[series_id] = {
                        "value": vintage.value,
                        "observed_on": vintage.observed_on.isoformat(),
                        "fetched_at": vintage.fetched_at.isoformat(),
                        "mom5": momentum(series[series_id], decision_at),
                    }
            btc_i = bisect.bisect_left(btc_times, decision_at) - 1
            if btc_i >= 0:
                state["BTC"] = {
                    "value": btc_values[btc_i],
                    "fetched_at": btc_times[btc_i].isoformat(),
                }

            spx = state.get("SP500", {})
            vix = state.get("VIXCLS", {})
            spread = state.get("T10Y2Y", {})
            state["regime"] = {
                "vix_level": (
                    "high" if vix.get("value", 0) >= 25
                    else "low" if vix.get("value", 99) < 15
                    else "mid"
                ) if vix else None,
                "spy_5d": (
                    trend_label(spx.get("mom5"))
                ) if spx else None,
                "curve": (
                    "inverted" if (spread.get("value") or 0) < 0 else "normal"
                ) if spread else None,
            }
            covered += 1

        batch.append((
            episode_id,
            t0,
            decision_at,
            coverage,
            len([key for key in state if key != "regime"]),
            Json(state),
        ))

    if not batch:
        conn.rollback()
        conn.close()
        raise RuntimeError("aucun épisode PIT; table market_state laissée intacte")

    write_cur.execute("DELETE FROM episode_market_state")
    execute_values(write_cur, """
        INSERT INTO episode_market_state
          (episode_id, t0, decision_at, coverage_ok, n_series, state)
        VALUES %s
    """, batch, page_size=2000)
    conn.commit()
    print(
        f"market_state reconstruits: {len(batch)}/{len(episodes)} | "
        f"pivots PIT frais: {covered}"
    )
    conn.close()


if __name__ == "__main__":
    main()
