#!/usr/bin/env python3
"""Builder 3 — outcomes daily sur les vraies sessions XNYS.

Deux règles fail-closed :

1. L'entrée est le premier close XNYS **strictement après** `decision_at`,
   c'est-à-dire l'instant où la représentation PIT de l'épisode est disponible.
2. Un horizon `k` signifie exactement `k` sessions XNYS. Si FRED ne contient pas
   le prix de la session d'entrée ou de sortie, l'outcome est rejeté ; on ne saute
   plus silencieusement vers une observation ultérieure.

Le calendrier versionné `exchange-calendars` fournit les closes UTC réels : heure
d'été/hiver, jours fériés, fermetures anticipées et fermetures exceptionnelles.
La table étant entièrement dérivée, elle est reconstruite atomiquement afin que
des lignes calculées avec l'ancienne approximation 21:00 UTC ne survivent pas.
"""
from __future__ import annotations

import bisect
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Mapping

import psycopg2
from psycopg2.extras import execute_values

PIPELINE_LOCK = "market-memory-build-pipeline-v2"
MIN_IN_RANGE_COVERAGE = 0.01
REPRESENTATION_VERSION = "pit-v2-observed-window-6h"


def _aware_utc(value: datetime, field: str = "timestamp") -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} doit être timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True)
class TradingSchedule:
    """Sessions ordonnées et heure de close exacte en UTC."""

    session_dates: tuple[date, ...]
    closes: tuple[datetime, ...]

    def __post_init__(self) -> None:
        if not self.session_dates or len(self.session_dates) != len(self.closes):
            raise ValueError("calendrier vide ou dates/closes incohérents")
        normalized = tuple(_aware_utc(close, "close") for close in self.closes)
        if normalized != self.closes:
            object.__setattr__(self, "closes", normalized)
        if any(a >= b for a, b in zip(self.session_dates, self.session_dates[1:])):
            raise ValueError("les dates de session doivent être strictement croissantes")
        if any(a >= b for a, b in zip(self.closes, self.closes[1:])):
            raise ValueError("les closes doivent être strictement croissants")

    def first_close_after(self, moment: datetime) -> tuple[int, date, datetime] | None:
        """Premier close strictement supérieur à `moment` (égalité => session suivante)."""
        moment = _aware_utc(moment, "decision_at")
        index = bisect.bisect_right(self.closes, moment)
        if index >= len(self.closes):
            return None
        return index, self.session_dates[index], self.closes[index]

    def span_after(
        self, moment: datetime, sessions: int,
    ) -> tuple[date, datetime, date, datetime] | None:
        """Sessions/closes d'entrée et de sortie à horizon exact."""
        if sessions < 1:
            raise ValueError("sessions doit être >= 1")
        entry = self.first_close_after(moment)
        if entry is None:
            return None
        entry_i, entry_date, entry_close = entry
        exit_i = entry_i + sessions
        if exit_i >= len(self.session_dates):
            return None
        return entry_date, entry_close, self.session_dates[exit_i], self.closes[exit_i]


@dataclass(frozen=True)
class MarketObservation:
    value: float
    fetched_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "fetched_at", _aware_utc(self.fetched_at, "fetched_at"))


@dataclass(frozen=True)
class ForwardOutcome:
    entry_date: date
    entry_value: float
    exit_value: float
    return_pct: float
    exit_date: date
    available_at: datetime

    @property
    def is_up(self) -> bool:
        return self.exit_value > self.entry_value


def load_xnys_schedule(start: date, end: date) -> TradingSchedule:
    """Charge XNYS depuis la bibliothèque versionnée, closes convertis en UTC."""
    if start > end:
        raise ValueError("borne calendrier inversée")
    try:
        import exchange_calendars as xcals
    except ImportError as exc:  # message actionnable pour un lancement manuel du builder
        raise RuntimeError(
            "exchange-calendars manque; installer requirements-builders.txt"
        ) from exc

    calendar = xcals.get_calendar("XNYS", start=start.isoformat(), end=end.isoformat())
    dates = tuple(label.date() for label in calendar.sessions)
    closes = tuple(
        close.to_pydatetime().astimezone(timezone.utc)
        for close in calendar.closes
    )
    return TradingSchedule(dates, closes)


def _value_and_availability(
    raw: float | MarketObservation, public_at: datetime,
) -> tuple[float, datetime]:
    if isinstance(raw, MarketObservation):
        return float(raw.value), max(raw.fetched_at, public_at)
    return float(raw), public_at


def forward_outcome(
    prices: Mapping[date, float | MarketObservation],
    schedule: TradingSchedule,
    decision_at: datetime,
    sessions: int,
) -> ForwardOutcome | None:
    """Return entre l'entrée et exactement `sessions` sessions XNYS plus tard."""
    span = schedule.span_after(decision_at, sessions)
    if span is None:
        return None
    entry_date, entry_close, exit_date, exit_close = span
    if entry_date not in prices or exit_date not in prices:
        return None
    entry_value, entry_available = _value_and_availability(prices[entry_date], entry_close)
    exit_value, exit_available = _value_and_availability(prices[exit_date], exit_close)
    if entry_value == 0:
        return None
    return ForwardOutcome(
        entry_date=entry_date,
        entry_value=entry_value,
        exit_value=exit_value,
        return_pct=round((exit_value / entry_value - 1) * 100, 3),
        exit_date=exit_date,
        available_at=max(exit_close, entry_available, exit_available),
    )


def change_between(
    values: Mapping[date, float | MarketObservation], entry_date: date, exit_date: date,
) -> float | None:
    """Variation absolue entre deux sessions précises ; aucune interpolation."""
    if entry_date not in values or exit_date not in values:
        return None
    entry = values[entry_date]
    exit_ = values[exit_date]
    entry_value = entry.value if isinstance(entry, MarketObservation) else float(entry)
    exit_value = exit_.value if isinstance(exit_, MarketObservation) else float(exit_)
    return round(float(exit_value) - float(entry_value), 3)


def load_series(cur, series_id: str) -> dict[date, MarketObservation]:
    cur.execute(
        """SELECT DISTINCT ON (date) date, value, fetched_at
           FROM fred_data
           WHERE series_id=%s AND value IS NOT NULL
             AND date IS NOT NULL AND fetched_at IS NOT NULL
           ORDER BY date, fetched_at""",
        (series_id,),
    )
    # Première vintage : valeur réellement connue le plus tôt, avec sa provenance.
    return {
        day: MarketObservation(float(value), fetched_at)
        for day, value, fetched_at in cur.fetchall()
    }


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

    spx = load_series(read_cur, "SP500")
    vix = load_series(read_cur, "VIXCLS")
    oil = load_series(read_cur, "DCOILWTICO")
    if not spx:
        raise RuntimeError("série SP500 vide; reconstruction refusée")
    spx_min_date, spx_max_date = min(spx), max(spx)

    # Marge pour résoudre proprement un event de week-end en bord de couverture.
    schedule = load_xnys_schedule(
        spx_min_date - timedelta(days=14), spx_max_date + timedelta(days=14),
    )

    read_cur.execute("""
        SELECT me.id, me.t0, er.as_of
        FROM market_episodes me
        JOIN episode_representations er ON er.episode_id = me.id
        WHERE me.kind <> 'recurring' AND er.representation_version = %s
        ORDER BY er.as_of, me.id
    """, [REPRESENTATION_VERSION])
    rows = read_cur.fetchall()
    batch: list[tuple] = []
    in_range = 0
    for episode_id, t0, decision_at in rows:
        three_span = schedule.span_after(decision_at, 3)
        if (
            three_span is not None
            and spx_min_date <= three_span[0] <= spx_max_date
            and spx_min_date <= three_span[2] <= spx_max_date
        ):
            in_range += 1
        one = forward_outcome(spx, schedule, decision_at, 1)
        three = forward_outcome(spx, schedule, decision_at, 3)
        seven = forward_outcome(spx, schedule, decision_at, 7)
        if three is None:
            continue
        one_span = schedule.span_after(decision_at, 1)
        if one_span is None:  # implique normalement `three is None`, défense explicite
            continue
        one_entry, _, one_exit, _ = one_span
        batch.append((
            episode_id,
            t0,
            decision_at,
            three.entry_date,
            one.return_pct if one else None,
            three.return_pct,
            seven.return_pct if seven else None,
            change_between(vix, one_entry, one_exit),
            change_between(oil, one_entry, one_exit),
            1 if three.is_up else 0,
            three.available_at,
        ))

    # Ne jamais effacer la table saine si une erreur de couverture produit zéro ligne.
    if not batch:
        conn.rollback()
        conn.close()
        raise RuntimeError(
            "aucun outcome strict calculable; table existante laissée intacte"
        )
    if in_range and len(batch) / in_range < MIN_IN_RANGE_COVERAGE:
        conn.rollback()
        conn.close()
        raise RuntimeError(
            f"couverture outcome anormale: {len(batch)}/{in_range} épisodes en plage; "
            "table existante laissée intacte"
        )

    # Reconstruction atomique de cette table 100 % dérivée : les lecteurs voient soit
    # l'ancienne version complète, soit la nouvelle après COMMIT, jamais un entre-deux.
    write_cur.execute("DELETE FROM episode_outcomes")
    execute_values(write_cur, """
        INSERT INTO episode_outcomes
          (episode_id, t0, decision_at, entry_date, spx_ret_1d, spx_ret_3d,
           spx_ret_7d, vix_chg_1d, oil_chg_1d, dir_3d, outcome_available_at)
        VALUES %s
    """, batch, page_size=2000)
    conn.commit()

    write_cur.execute("""
        SELECT count(*), round(avg(dir_3d)::numeric,3),
               round(avg(spx_ret_3d)::numeric,3)
        FROM episode_outcomes
    """)
    count, up_probability, mean_return = write_cur.fetchone()
    print(
        f"outcomes XNYS stricts: {count}/{len(rows)} | "
        f"P(up 3 sessions)={up_probability} | moy 3 sessions={mean_return}%"
    )
    conn.close()


if __name__ == "__main__":
    main()
