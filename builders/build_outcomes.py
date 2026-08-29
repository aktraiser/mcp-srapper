#!/usr/bin/env python3
"""Builder 3 — OUTCOMES leakage-safe (résolution jour, MVP).

Entrée = 1er close SP500 STRICTEMENT APRÈS l'instant t0 (on agit APRÈS avoir vu l'info) :
si l'event tombe après la clôture US (~21:00 UTC), le close du jour est déjà imprimé, donc
on entre le jour de bourse suivant (sinon on « entrerait » à un prix d'AVANT l'info -> fuite).
Sortie = +k pas de bourse. On N'ÉCRIT QUE les épisodes propres :
  - t0 dans la fenêtre de la série,
  - entrée à <= MAXGAP jours d'un vrai point (sinon la série est trop trouée -> rejet).
Sans cette garde, les épisodes hors couverture produisent des returns bidons
(bisect clampé au bord) qui polluent tout : P(up) passe de 0.26 (garbage) à ~0.52 (sain).

Limite connue : FRED est daily et clairsemé -> per-stock/intraday = Alpaca (voir README).
DSN lu depuis l'env EVENTS_DSN (rôle EN ÉCRITURE requis, pas mcp_ro qui est read-only).
Idempotent + reproductible : ON CONFLICT DO UPDATE -> un re-run BACKFILLE les colonnes
(dont outcome_available_at) sur les lignes préexistantes ; DO NOTHING les laisserait NULL.
"""
from __future__ import annotations
import bisect
import os
from datetime import datetime, time, timedelta, timezone

import psycopg2
from psycopg2.extras import execute_values

DSN = os.environ["EVENTS_DSN"]
MAXGAP_DAYS = 3
US_CLOSE = time(21, 0)  # ~clôture US en UTC : l'outcome à 3j est connu après ce close


def load_series(cur, series_id: str):
    cur.execute("""SELECT date, value FROM fred_data
                   WHERE series_id=%s AND value IS NOT NULL ORDER BY date, fetched_at""", (series_id,))
    seen = {d: float(v) for d, v in cur.fetchall()}
    ds = sorted(seen)
    return ds, [seen[d] for d in ds]


def main() -> None:
    conn = psycopg2.connect(DSN, connect_timeout=15)
    rc, wc = conn.cursor(), conn.cursor()
    spx_d, spx_v = load_series(rc, "SP500")
    vix_d, vix_v = load_series(rc, "VIXCLS")
    oil_d, oil_v = load_series(rc, "DCOILWTICO")

    def entry_floor(t0):
        """Date-plancher de l'ENTRÉE : 1er close utilisable est APRÈS l'info.
        Event après la clôture US -> on décale au jour suivant."""
        tt = t0.astimezone(timezone.utc) if t0.tzinfo is not None else t0
        d = tt.date()
        if tt.time() >= US_CLOSE:
            d = d + timedelta(days=1)
        return d

    def fwd(ds, vs, t0, k):
        d = entry_floor(t0)
        if d < ds[0] or d > ds[-1 - k]:
            return None, None, None, None
        i = bisect.bisect_left(ds, d)  # 1er close >= plancher (donc strictement après t0)
        if i >= len(ds) or i + k >= len(ds) or abs((ds[i] - d).days) > MAXGAP_DAYS:
            return None, None, None, None
        # exit_date = date du close au k-ième jour de bourse -> dispo de l'outcome
        return ds[i], vs[i], round((vs[i + k] / vs[i] - 1) * 100, 3), ds[i + k]

    def chg1(ds, vs, t0):
        d = entry_floor(t0)
        i = bisect.bisect_left(ds, d)
        if i >= len(ds) or i + 1 >= len(ds) or abs((ds[i] - d).days) > MAXGAP_DAYS:
            return None
        return round(vs[i + 1] - vs[i], 3)

    rc.execute("SELECT id, t0 FROM market_episodes WHERE kind <> 'recurring'")
    batch, kept = [], 0
    for eid, t0 in rc.fetchall():
        entry_date, _, r1, _ = fwd(spx_d, spx_v, t0, 1)
        _, _, r3, avail3 = fwd(spx_d, spx_v, t0, 3)
        _, _, r7, _ = fwd(spx_d, spx_v, t0, 7)
        if r3 is None:                             # garde de couverture/densité
            continue
        # instant où l'outcome à 3j est connu = close du 3e jour de bourse
        outcome_available_at = datetime.combine(avail3, US_CLOSE, tzinfo=timezone.utc)
        batch.append((eid, t0, entry_date, r1, r3, r7,
                      chg1(vix_d, vix_v, t0), chg1(oil_d, oil_v, t0),
                      1 if r3 > 0 else 0, outcome_available_at))
        kept += 1

    execute_values(wc, """
        INSERT INTO episode_outcomes
          (episode_id, t0, entry_date, spx_ret_1d, spx_ret_3d, spx_ret_7d, vix_chg_1d, oil_chg_1d,
           dir_3d, outcome_available_at)
        VALUES %s ON CONFLICT (episode_id) DO UPDATE SET
          t0 = EXCLUDED.t0, entry_date = EXCLUDED.entry_date,
          spx_ret_1d = EXCLUDED.spx_ret_1d, spx_ret_3d = EXCLUDED.spx_ret_3d,
          spx_ret_7d = EXCLUDED.spx_ret_7d, vix_chg_1d = EXCLUDED.vix_chg_1d,
          oil_chg_1d = EXCLUDED.oil_chg_1d, dir_3d = EXCLUDED.dir_3d,
          outcome_available_at = EXCLUDED.outcome_available_at
    """, batch, page_size=2000)
    conn.commit()
    # Stat sur le sous-ensemble LEAKAGE-SAFE uniquement (outcome_available_at renseigné) :
    # sinon d'anciennes lignes legacy sans cette colonne polluent la moyenne globale.
    wc.execute("""SELECT count(*), round(avg(dir_3d)::numeric,3), round(avg(spx_ret_3d)::numeric,3)
                  FROM episode_outcomes WHERE outcome_available_at IS NOT NULL""")
    n_safe, up, mu = wc.fetchone()
    print(f"outcomes propres (ce run): {kept} | leakage-safe en base: {n_safe} | P(up 3j)={up} | moy 3j={mu}%")
    conn.close()


if __name__ == "__main__":
    main()
