#!/usr/bin/env python3
"""Builder 1 — MarketEpisode : 1 épisode par event, t0 = onset du burst (sans leakage).

Classe chaque cluster `events` :
  - point         : burst net, span court
  - saga_primary  : multi-burst (à re-segmenter en N épisodes, phase 2)
  - recurring     : topic plat/récurrent (bruit) -> gardé mais filtrable en aval

t0 = première publication du burst dominant (instant où l'info devient publique),
PAS le first_seen du cluster (qui, pour un sujet d'anticipation, ancre sur la rumeur).

DSN lu depuis l'env EVENTS_DSN (jamais en dur). Idempotent (ON CONFLICT DO NOTHING).
"""
from __future__ import annotations
import os
import statistics as st
from collections import Counter, defaultdict
from datetime import timedelta

import psycopg2
from psycopg2.extras import execute_values

DSN = os.environ["EVENTS_DSN"]


def onset(items: list[tuple]) -> tuple:
    """items = [(published_at, source_domain)]. Retourne (t0, kind, multi, peak, burstiness, span_h, n, n_domains)."""
    times = sorted(t for t, _ in items)
    domains = [d for _, d in items]
    span_h = (times[-1] - times[0]).total_seconds() / 3600.0
    n, nd = len(times), len(set(domains))
    if n < 3:
        return times[0], "point", False, n, 1.0, round(span_h, 1), n, nd

    base = times[0].replace(minute=0, second=0, microsecond=0)
    idx = [int((t - base).total_seconds() // 3600) for t in times]
    cnt = Counter(idx)
    H = max(idx) + 1
    counts = [cnt.get(i, 0) for i in range(H)]
    peak = max(counts)
    med = st.median(counts) or 1
    burst = peak / max(1.0, med)
    thr = max(3, 2 * med)

    # récurrent : long + plat (aucun vrai burst)
    if span_h > 72 and peak < max(3, 3 * med):
        return times[0], "recurring", False, peak, round(burst, 2), round(span_h, 1), n, nd

    peak_i = counts.index(peak)
    i = peak_i
    while i > 0 and counts[i - 1] >= thr:          # remonter au début du run
        i -= 1
    # nombre de bursts distincts (pics locaux au-dessus du seuil) -> saga si > 1
    bursts = sum(1 for j in range(H) if counts[j] >= thr and counts[j] == max(counts[max(0, j - 3):j + 4]))
    onset_lo = base + timedelta(hours=i)
    t0 = min((t for t in times if t >= onset_lo), default=times[0])
    kind = "saga_primary" if bursts > 1 else "point"
    return t0, kind, bursts > 1, peak, round(burst, 2), round(span_h, 1), n, nd


def main() -> None:
    conn = psycopg2.connect(DSN, connect_timeout=15)
    rc, wc = conn.cursor(), conn.cursor()

    rc.execute("""SELECT id, COALESCE(event_type, signature_type), signature_type,
                         main_theme, main_entities, signature FROM events""")
    meta = {r[0]: r[1:] for r in rc.fetchall()}

    grp: dict = defaultdict(list)
    rc.execute("""SELECT event_id, published_at, source_domain FROM articles
                  WHERE event_id IS NOT NULL AND published_at IS NOT NULL""")
    for ev, pa, dom in rc.fetchall():
        grp[ev].append((pa, dom))

    batch = []
    for ev, items in grp.items():
        t0, kind, multi, peak, burst, span, n, nd = onset(items)
        m = meta.get(ev, (None, None, None, None, None))
        batch.append((ev, t0, kind, multi, n, nd, span, peak, burst, m[0], m[1], m[2], m[3], m[4]))

    execute_values(wc, """
        INSERT INTO market_episodes
          (source_event_id, t0, kind, has_multi_burst, n_articles, n_domains, span_h,
           burst_peak, burstiness, event_type, signature_type, main_theme, main_entities, signature)
        VALUES %s ON CONFLICT (source_event_id) DO NOTHING
    """, batch, page_size=2000)
    conn.commit()

    wc.execute("SELECT kind, count(*) FROM market_episodes GROUP BY 1 ORDER BY 2 DESC")
    print("episodes:", len(batch), "|", dict(wc.fetchall()))
    conn.close()


if __name__ == "__main__":
    main()
