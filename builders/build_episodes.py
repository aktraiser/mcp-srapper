#!/usr/bin/env python3
"""Builder 1 — épisodes et représentations sémantiques point-in-time.

L'ancien builder calculait l'onset avec toute la vie future du cluster, puis
`find_analogs` relisait le centroïde mutable de `events`. Les deux opérations
réécrivaient donc le passé.

La règle v2 est explicite et reproductible :

* `observed_at = max(published_at, collected_at)` ;
* pour les nouvelles lignes, on inclut aussi `event_assigned_at` dans la disponibilité ;
* on attend une fenêtre fixe de six heures ;
* seuls les articles observés avant la fin de cette fenêtre peuvent définir
  l'onset, les métadonnées et le centroïde ;
* la liste des articles et le centroïde sont ensuite immuables
  (`ON CONFLICT DO NOTHING` dans `episode_representations`).

Ainsi, un article futur ou un article antidaté mais collecté tard ne peut jamais
modifier un épisode déjà gelé. `t0` reste l'onset narratif ; `as_of` est le cutoff.
Les données historiques dépourvues d'horodatage d'assignation sont explicitement
marquées `historical_reconstruction_unversioned_cluster`, jamais présentées comme
une preuve temporelle impossible à reconstituer. Elles conservent les métadonnées
de provenance mais **aucun embedding** et sont exclues du retrieval strict.

DSN lu depuis `EVENTS_DSN` uniquement dans `main()` afin que les fonctions pures
restent testables sans base.
"""
from __future__ import annotations

import os
import statistics as st
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

import psycopg2
from psycopg2.extras import Json, execute_values

OBSERVATION_WINDOW = timedelta(hours=6)
REPRESENTATION_METHOD = "observed-window-6h-complete-article-mean-v2"
REPRESENTATION_VERSION = "pit-v2-observed-window-6h"
BUILDER_VERSION = "v2-pit-6h"
PIPELINE_LOCK = "market-memory-build-pipeline-v2"

# Même taxonomie publique que le MCP. Une valeur article inconnue reste dans
# `signature_type`, mais n'est pas promue comme `event_type` arbitraire.
EVENT_TYPES = {
    "announcement", "earnings", "acquisition", "regulation", "decision",
    "incident", "trend", "partnership", "product", "legal", "forecast",
}


def _aware_utc(value: datetime, field: str) -> datetime:
    """Normalise un timestamp conscient ; rejette le naïf plutôt que d'inventer sa TZ."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} doit être timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True)
class ArticleObservation:
    id: Any
    published_at: datetime
    collected_at: datetime | None
    source_domain: str | None = None
    title: str | None = None
    theme: str | None = None
    event_hint: str | None = None
    entities: Any = None
    relevance_score: float | None = None
    source_tier: int | None = None
    event_assigned_at: datetime | None = None
    membership_valid_to: datetime | None = None
    embedding_generated_at: datetime | None = None
    has_embedding: bool = False

    @property
    def observed_at(self) -> datetime:
        published = _aware_utc(self.published_at, "published_at")
        if self.collected_at is None:
            raise ValueError("collected_at manque; disponibilité impossible à prouver")
        collected = _aware_utc(self.collected_at, "collected_at")
        return max(published, collected)

    @property
    def membership_available_at(self) -> datetime | None:
        """Disponibilité prouvée du contenu ET de son appartenance au cluster."""
        if self.event_assigned_at is None:
            return None
        return max(
            self.observed_at,
            _aware_utc(self.event_assigned_at, "event_assigned_at"),
        )

    def membership_active_at(self, moment: datetime) -> bool:
        """L'article appartient encore à cet event au cutoff considéré."""
        moment = _aware_utc(moment, "membership cutoff")
        available = self.membership_available_at
        return (available is None or available <= moment) and (
            self.membership_valid_to is None
            or _aware_utc(self.membership_valid_to, "membership_valid_to") > moment
        )


@dataclass(frozen=True)
class FrozenSnapshot:
    first_observed_at: datetime
    source_cutoff: datetime
    as_of: datetime
    articles: tuple[ArticleObservation, ...]
    provenance_status: str


def freeze_snapshot(
    articles: Iterable[ArticleObservation],
    now: datetime,
    window: timedelta = OBSERVATION_WINDOW,
    force_unverified: bool = False,
) -> FrozenSnapshot | None:
    """Sélectionne la représentation disponible à `first_observed + window`.

    Retourne ``None`` tant que la fenêtre n'est pas terminée. Le tri rend la
    provenance stable, même si PostgreSQL renvoie les articles dans un autre ordre.
    """
    if window <= timedelta(0):
        raise ValueError("la fenêtre d'observation doit être positive")
    now = _aware_utc(now, "now")
    observed_order = sorted(
        articles,
        key=lambda a: (a.observed_at, _aware_utc(a.published_at, "published_at"), str(a.id)),
    )
    if not observed_order:
        return None
    first_observed = observed_order[0].observed_at
    historical_cutoff = first_observed + window
    historical_eligible = tuple(
        article for article in observed_order
        if article.observed_at <= historical_cutoff
        and article.membership_active_at(historical_cutoff)
    )
    if not historical_eligible:
        return None

    # À partir de la migration 003, le trigger source horodate l'assignation event_id.
    # Si chaque article de la première fenêtre possède cette preuve, la fenêtre est
    # ancrée sur la disponibilité réelle de l'appartenance au cluster.
    if (
        not force_unverified
        and all(article.membership_available_at is not None for article in observed_order)
    ):
        membership_order = sorted(
            (article for article in observed_order if article.membership_available_at is not None),
            key=lambda article: (
                article.membership_available_at,
                article.observed_at,
                str(article.id),
            ),
        )
        first_available = membership_order[0].membership_available_at
        cutoff = first_available + window
        if cutoff > now:
            return None
        eligible = tuple(
            article for article in membership_order
            if article.membership_available_at <= cutoff
            and article.membership_active_at(cutoff)
        )
        if not eligible:
            return None
        return FrozenSnapshot(
            first_observed_at=min(article.observed_at for article in eligible),
            source_cutoff=cutoff,
            # Tous les inputs sont visibles dans le snapshot REPEATABLE READ ouvert
            # à `now`. L'ancrer ici (et non au timestamp du statement source) évite
            # de considérer connue une écriture qui n'était pas encore COMMIT à cutoff.
            as_of=now,
            articles=eligible,
            provenance_status="verified_feature_timestamps",
        )

    if historical_cutoff > now:
        return None
    return FrozenSnapshot(
        first_observed_at=first_observed,
        source_cutoff=historical_cutoff,
        as_of=now,
        articles=historical_eligible,
        provenance_status="historical_reconstruction_unversioned_cluster",
    )


def onset(items: list[tuple[datetime, str]]) -> tuple:
    """Retourne (t0, kind, multi, peak, burstiness, span_h, n, n_domains).

    `items` doit déjà être limité à la fenêtre PIT par :func:`freeze_snapshot`.
    """
    if not items:
        raise ValueError("onset exige au moins un article")
    times = sorted(_aware_utc(t, "published_at") for t, _ in items)
    domains = [d for _, d in items]
    span_h = (times[-1] - times[0]).total_seconds() / 3600.0
    n, nd = len(times), len(set(d for d in domains if d))
    if n < 3:
        return times[0], "point", False, n, 1.0, round(span_h, 1), n, nd

    base = times[0].replace(minute=0, second=0, microsecond=0)
    idx = [int((t - base).total_seconds() // 3600) for t in times]
    cnt = Counter(idx)
    hour_count = max(idx) + 1
    counts = [cnt.get(i, 0) for i in range(hour_count)]
    peak = max(counts)
    med = st.median(counts) or 1
    burst = peak / max(1.0, med)
    threshold = max(3, 2 * med)

    if span_h > 72 and peak < max(3, 3 * med):
        return times[0], "recurring", False, peak, round(burst, 2), round(span_h, 1), n, nd

    peak_i = counts.index(peak)
    start_i = peak_i
    while start_i > 0 and counts[start_i - 1] >= threshold:
        start_i -= 1
    bursts = sum(
        1 for i in range(hour_count)
        if counts[i] >= threshold
        and counts[i] == max(counts[max(0, i - 3):i + 4])
    )
    onset_lo = base + timedelta(hours=start_i)
    t0 = min((t for t in times if t >= onset_lo), default=times[0])
    kind = "saga_primary" if bursts > 1 else "point"
    return t0, kind, bursts > 1, peak, round(burst, 2), round(span_h, 1), n, nd


def _mode(values: Iterable[str | None]) -> str | None:
    cleaned = [v.strip() for v in values if isinstance(v, str) and v.strip()]
    if not cleaned:
        return None
    counts = Counter(cleaned)
    return min(counts, key=lambda v: (-counts[v], v.casefold(), v))


def _entity_labels(raw: Any) -> list[str]:
    """Extrait des libellés courts depuis le JSON hétérogène des articles."""
    if raw is None:
        return []
    if isinstance(raw, str):
        value = raw.strip()
        return [value] if value else []
    if isinstance(raw, dict):
        for key in ("name", "text", "symbol", "ticker", "entity", "value"):
            if isinstance(raw.get(key), str) and raw[key].strip():
                return [raw[key].strip()]
        return []
    if isinstance(raw, (list, tuple)):
        return [label for item in raw for label in _entity_labels(item)]
    return []


def snapshot_metadata(snapshot: FrozenSnapshot) -> tuple[str | None, str | None, str | None, list[str], str | None]:
    """Métadonnées connues à `snapshot.as_of`, sans relire l'event mature."""
    articles = snapshot.articles
    # `articles.signal_type` est un sentiment (positive/negative/neutral), pas un
    # type d'événement. `event_hint` vient uniquement de l'analyse article connue
    # lors de sa collecte (`analysis.type`) et reste NULL si l'amont ne l'a pas stockée.
    signature_type = _mode(a.event_hint for a in articles)
    event_type = signature_type if signature_type in EVENT_TYPES else None
    main_theme = _mode(a.theme for a in articles)

    entity_counts = Counter(
        label for article in articles for label in _entity_labels(article.entities)
    )
    main_entities = sorted(
        entity_counts,
        key=lambda label: (-entity_counts[label], label.casefold(), label),
    )[:20]

    representative = min(
        articles,
        key=lambda a: (
            -(float(a.relevance_score) if a.relevance_score is not None else -1.0),
            int(a.source_tier) if a.source_tier is not None else 99,
            a.observed_at,
            str(a.id),
        ),
    )
    signature = representative.title.strip() if representative.title and representative.title.strip() else None
    return event_type, signature_type, main_theme, main_entities, signature


def _max_timestamp(values: Iterable[datetime | None], field: str) -> datetime | None:
    normalized = [_aware_utc(value, field) for value in values if value is not None]
    return max(normalized) if normalized else None


def _dsn() -> str:
    value = os.getenv("EVENTS_DSN", "").strip()
    if not value:
        raise RuntimeError("EVENTS_DSN manque (rôle DB en écriture requis)")
    return value


def main() -> None:
    conn = psycopg2.connect(_dsn(), connect_timeout=15)
    conn.set_session(isolation_level="REPEATABLE READ")
    read_cur, write_cur = conn.cursor(), conn.cursor()

    # Un seul freezer à la fois. Le lock est relâché automatiquement avec la transaction.
    read_cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", [PIPELINE_LOCK])
    # Force l'acquisition du snapshot REPEATABLE READ, puis horodate APRÈS celle-ci.
    # Toute ligne visible par les SELECT suivants était donc déjà commitée avant
    # `build_now`; transaction_timestamp() serait légèrement trop tôt.
    read_cur.execute("SELECT 1 FROM articles LIMIT 0")
    read_cur.execute("SELECT clock_timestamp()")
    build_now = read_cur.fetchone()[0]

    # Une représentation déjà présente est immuable : aucun re-run ne la recalcule.
    read_cur.execute("""
        SELECT me.source_event_id
        FROM market_episodes me
        JOIN episode_representations er ON er.episode_id = me.id
    """)
    frozen_events = {row[0] for row in read_cur.fetchall()}

    read_cur.execute("SELECT event_id FROM legacy_event_taints")
    legacy_tainted_events = {row[0] for row in read_cur.fetchall()}

    grouped: dict[Any, list[ArticleObservation]] = defaultdict(list)
    read_cur.execute("""
        WITH memberships AS (
          -- Intervalles réellement observés depuis la migration 003, ouverts ou clos.
          SELECT m.event_id, a.id, a.published_at, a.collected_at,
                 a.source_domain, a.title, a.theme,
                 a.analysis->>'type' AS event_hint, a.entities,
                 a.relevance_score, a.source_tier, m.valid_from AS event_assigned_at,
                 m.valid_to AS membership_valid_to, a.embedding_generated_at,
                 (a.embedding IS NOT NULL) AS has_embedding
          FROM article_event_memberships m
          JOIN articles a ON a.id = m.article_id
          WHERE a.published_at IS NOT NULL AND a.collected_at IS NOT NULL

          UNION ALL

          -- Appartenance courante legacy sans intervalle prouvé : utile seulement pour
          -- l'audit historique, jamais pour produire un embedding strict.
          SELECT a.event_id, a.id, a.published_at, a.collected_at,
                 a.source_domain, a.title, a.theme,
                 a.analysis->>'type' AS event_hint, a.entities,
                 a.relevance_score, a.source_tier, NULL::timestamptz,
                 NULL::timestamptz, a.embedding_generated_at,
                 (a.embedding IS NOT NULL) AS has_embedding
          FROM articles a
          WHERE a.event_id IS NOT NULL
            AND a.published_at IS NOT NULL
            AND a.collected_at IS NOT NULL
            AND NOT EXISTS (
              SELECT 1 FROM article_event_memberships m
              WHERE m.article_id = a.id AND m.event_id = a.event_id
                AND m.valid_to IS NULL
            )
        )
        SELECT event_id, id, published_at, collected_at, source_domain, title,
               theme, event_hint, entities, relevance_score, source_tier,
               event_assigned_at, membership_valid_to,
               embedding_generated_at, has_embedding
        FROM memberships
    """)
    for row in read_cur.fetchall():
        event_id = row[0]
        if event_id in frozen_events:
            continue
        grouped[event_id].append(ArticleObservation(*row[1:]))

    episode_rows: list[tuple] = []
    representation_rows: list[tuple] = []
    pending = 0
    for event_id, articles in grouped.items():
        snapshot = freeze_snapshot(
            articles,
            build_now,
            force_unverified=event_id in legacy_tainted_events,
        )
        if snapshot is None:
            pending += 1
            continue
        t0, kind, multi, peak, burst, span, n, n_domains = onset([
            (article.published_at, article.source_domain or "")
            for article in snapshot.articles
        ])
        event_type, signature_type, theme, entities, signature = snapshot_metadata(snapshot)
        episode_rows.append((
            event_id, t0, kind, multi, n, n_domains, span, peak, burst,
            event_type, signature_type, theme, entities, signature, BUILDER_VERSION,
        ))
        ids = [article.id for article in snapshot.articles]
        representation_rows.append((
            event_id,
            snapshot.first_observed_at,
            snapshot.source_cutoff,
            snapshot.as_of,
            ids,
            len(ids),
            Json([
                {
                    "article_id": str(article.id),
                    "valid_from": (
                        _aware_utc(article.event_assigned_at, "event_assigned_at").isoformat()
                        if article.event_assigned_at is not None else None
                    ),
                    "valid_to": (
                        _aware_utc(article.membership_valid_to, "membership_valid_to").isoformat()
                        if article.membership_valid_to is not None else None
                    ),
                }
                for article in snapshot.articles
            ]),
            max(_aware_utc(a.published_at, "published_at") for a in snapshot.articles),
            max(a.observed_at for a in snapshot.articles),
            REPRESENTATION_METHOD,
            REPRESENTATION_VERSION,
            _max_timestamp(
                (a.event_assigned_at for a in snapshot.articles),
                "event_assigned_at",
            ),
            _max_timestamp(
                (a.embedding_generated_at for a in snapshot.articles if a.has_embedding),
                "embedding_generated_at",
            ),
            snapshot.provenance_status,
        ))

    if not episode_rows:
        conn.rollback()
        print(f"aucun nouvel épisode à figer | fenêtres encore ouvertes: {pending}")
        conn.close()
        return

    # Les lignes legacy sans représentation sont remplacées une seule fois par leur
    # version PIT. Dès que la représentation est insérée, les re-runs les ignorent.
    execute_values(write_cur, """
        INSERT INTO market_episodes
          (source_event_id, t0, kind, has_multi_burst, n_articles, n_domains, span_h,
           burst_peak, burstiness, event_type, signature_type, main_theme, main_entities,
           signature, builder_version)
        VALUES %s
        ON CONFLICT (source_event_id) DO UPDATE SET
          t0 = EXCLUDED.t0,
          kind = EXCLUDED.kind,
          has_multi_burst = EXCLUDED.has_multi_burst,
          n_articles = EXCLUDED.n_articles,
          n_domains = EXCLUDED.n_domains,
          span_h = EXCLUDED.span_h,
          burst_peak = EXCLUDED.burst_peak,
          burstiness = EXCLUDED.burstiness,
          event_type = EXCLUDED.event_type,
          signature_type = EXCLUDED.signature_type,
          main_theme = EXCLUDED.main_theme,
          main_entities = EXCLUDED.main_entities,
          signature = EXCLUDED.signature,
          builder_version = EXCLUDED.builder_version
    """, episode_rows, page_size=1000)

    # Le centroïde est calculé dans PostgreSQL pour ne jamais transférer des centaines
    # de milliers de vecteurs 1536D en Python. Il n'est produit que si la disponibilité
    # de CHAQUE embedding est prouvée avant source_cutoff. Une reconstruction historique garde
    # ses IDs pour audit, mais son vecteur reste volontairement NULL.
    execute_values(write_cur, """
        WITH snapshots(
          source_event_id, first_observed_at, source_cutoff, as_of, source_article_ids,
          source_article_count, source_membership_intervals,
          source_max_published_at, source_max_observed_at,
          method, representation_version, source_max_event_assigned_at,
          source_max_embedding_generated_at, provenance_status
        ) AS (VALUES %s)
        INSERT INTO episode_representations
          (episode_id, first_observed_at, source_cutoff, as_of, embedding, source_article_ids,
           source_article_count, source_membership_intervals,
           embedded_article_ids, embedded_article_count,
           source_max_published_at, source_max_observed_at, method,
           representation_version, source_max_event_assigned_at,
           source_max_embedding_generated_at, provenance_status)
        SELECT me.id, s.first_observed_at, s.source_cutoff, s.as_of, vectors.embedding,
               s.source_article_ids, s.source_article_count,
               s.source_membership_intervals, vectors.embedded_ids,
               vectors.embedded_count,
               s.source_max_published_at, s.source_max_observed_at, s.method,
               s.representation_version, s.source_max_event_assigned_at,
               s.source_max_embedding_generated_at, s.provenance_status
        FROM snapshots s
        JOIN market_episodes me ON me.source_event_id = s.source_event_id
        CROSS JOIN LATERAL (
          SELECT CASE
                   WHEN s.provenance_status = 'verified_feature_timestamps'
                    AND count(a.embedding) FILTER (
                      WHERE a.embedding_generated_at IS NOT NULL
                        AND a.embedding_generated_at <= s.source_cutoff
                    ) = s.source_article_count
                   THEN avg(a.embedding)
                     FILTER (
                       WHERE a.embedding_generated_at IS NOT NULL
                         AND a.embedding_generated_at <= s.source_cutoff
                     )
                   ELSE NULL
                 END AS embedding,
                 COALESCE(
                   array_agg(a.id ORDER BY a.id) FILTER (
                     WHERE s.provenance_status = 'verified_feature_timestamps'
                       AND a.embedding IS NOT NULL
                       AND a.embedding_generated_at IS NOT NULL
                       AND a.embedding_generated_at <= s.source_cutoff
                   ),
                   ARRAY[]::uuid[]
                 ) AS embedded_ids,
                 count(a.embedding) FILTER (
                   WHERE s.provenance_status = 'verified_feature_timestamps'
                     AND a.embedding_generated_at IS NOT NULL
                     AND a.embedding_generated_at <= s.source_cutoff
                 )::int AS embedded_count
          FROM articles a
          WHERE a.id = ANY(s.source_article_ids)
        ) vectors
        ON CONFLICT (episode_id) DO NOTHING
    """, representation_rows, page_size=500, template=(
        "(%s::uuid, %s::timestamptz, %s::timestamptz, %s::timestamptz, %s::uuid[], %s::int, "
        "%s::jsonb, %s::timestamptz, %s::timestamptz, %s::text, %s::text, "
        "%s::timestamptz, %s::timestamptz, %s::text)"
    ))

    conn.commit()
    write_cur.execute("""
        SELECT count(*), count(*) FILTER (WHERE embedding IS NOT NULL)
        FROM episode_representations
    """)
    total, with_embedding = write_cur.fetchone()
    print(
        f"épisodes figés ce run: {len(episode_rows)} | fenêtres ouvertes: {pending} | "
        f"représentations en base: {total} | avec embedding: {with_embedding}"
    )
    conn.close()


if __name__ == "__main__":
    main()
