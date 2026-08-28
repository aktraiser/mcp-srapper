#!/usr/bin/env python3
"""Builder 2 — MARKET STATE @ t0, point-in-time (dernière valeur STRICTEMENT avant t0).

Sensor depuis la DB : FRED (SP500, VIX, WTI, USD, spread 10Y-2Y, Fed funds, natgas)
+ crypto BTC. Résolution jour (limite connue : FRED est clairsemé -> voir README).
coverage_ok = t0 dans la fenêtre de données marché.

DSN lu depuis l'env EVENTS_DSN. Idempotent (ON CONFLICT DO NOTHING).
"""
from __future__ import annotations
import bisect
import os
from datetime import date, timedelta

import psycopg2
from psycopg2.extras import Json, execute_values

DSN = os.environ["EVENTS_DSN"]
SERIES = ["SP500", "VIXCLS", "DCOILWTICO", "DTWEXBGS", "T10Y2Y", "DFF", "DHHNGSP"]


def main() -> None:
    conn = psycopg2.connect(DSN, connect_timeout=15)
    rc, wc = conn.cursor(), conn.cursor()

    ser: dict = {}
    for s in SERIES:
        rc.execute("""SELECT date, value FROM fred_data
                      WHERE series_id=%s AND value IS NOT NULL ORDER BY date, fetched_at""", (s,))
        seen = {d: float(v) for d, v in rc.fetchall()}   # dernière valeur par date
        ds = sorted(seen)
        ser[s] = (ds, [seen[d] for d in ds])

    rc.execute("SELECT fetched_at, price FROM crypto_data WHERE symbol='BTC' AND price IS NOT NULL ORDER BY fetched_at")
    btc = rc.fetchall()
    btc_t = [r[0] for r in btc]
    btc_v = [float(r[1]) for r in btc]

    cov_min = max(ser["SP500"][0][0], ser["VIXCLS"][0][0])

    def pit(s: str, d: date):                     # dernière valeur strictement avant la date d
        ds, vs = ser[s]
        i = bisect.bisect_left(ds, d) - 1
        return (vs[i], ds[i]) if i >= 0 else (None, None)

    def mom(s: str, d: date, days: int = 7):
        v0, _ = pit(s, d)
        vp, _ = pit(s, d - timedelta(days=days))
        return round((v0 / vp - 1) * 100, 2) if (v0 and vp) else None

    rc.execute("SELECT id, t0 FROM market_episodes WHERE kind <> 'recurring'")
    batch, ok = [], 0
    for eid, t0 in rc.fetchall():
        d = t0.date()
        coverage = cov_min <= d <= date(2100, 1, 1)
        state: dict = {}
        if coverage:
            for s in SERIES:
                v, _ = pit(s, d)
                if v is not None:
                    state[s] = {"value": v, "mom5": mom(s, d)}
            i = bisect.bisect_left(btc_t, t0) - 1
            if i >= 0:
                state["BTC"] = {"value": btc_v[i]}
            sp, vix, spr = state.get("SP500", {}), state.get("VIXCLS", {}), state.get("T10Y2Y", {})
            state["regime"] = {
                "vix_level": (("high" if vix.get("value", 0) >= 25 else "low" if vix.get("value", 99) < 15 else "mid") if vix else None),
                "spy_5d": (("up" if (sp.get("mom5") or 0) > 0 else "down") if sp else None),
                "curve": (("inverted" if (spr.get("value") or 0) < 0 else "normal") if spr else None),
            }
            ok += 1
        batch.append((eid, t0, coverage, len([k for k in state if k != "regime"]), Json(state)))

    execute_values(wc, """
        INSERT INTO episode_market_state (episode_id, t0, coverage_ok, n_series, state)
        VALUES %s ON CONFLICT (episode_id) DO NOTHING
    """, batch, page_size=2000)
    conn.commit()
    print(f"market_state écrits: {len(batch)} | en couverture: {ok} | début data: {cov_min}")
    conn.close()


if __name__ == "__main__":
    main()
