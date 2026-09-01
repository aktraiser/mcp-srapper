#!/usr/bin/env python3
"""Audit read-only à exécuter avant de redémarrer le MCP v2."""
from __future__ import annotations

import os

import psycopg2

REPRESENTATION_VERSION = "pit-v2-observed-window-6h"


def _dsn() -> str:
    value = os.getenv("EVENTS_DSN", "").strip()
    if not value:
        raise RuntimeError("EVENTS_DSN manque")
    return value


def scalar(cur, sql: str, params: tuple = ()) -> int:
    cur.execute(sql, params)
    return int(cur.fetchone()[0])


def main() -> None:
    conn = psycopg2.connect(_dsn(), connect_timeout=15)
    conn.set_session(readonly=True, isolation_level="REPEATABLE READ")
    cur = conn.cursor()

    stats = {
        "episodes_non_recurring": scalar(
            cur, "SELECT count(*) FROM market_episodes WHERE kind <> 'recurring'",
        ),
        "representations": scalar(
            cur,
            "SELECT count(*) FROM episode_representations WHERE representation_version=%s",
            (REPRESENTATION_VERSION,),
        ),
        "representations_usable": scalar(
            cur,
            """SELECT count(*) FROM episode_representations
               WHERE representation_version=%s AND embedding IS NOT NULL
                 AND provenance_status='verified_feature_timestamps'
                 AND embedded_article_count=source_article_count""",
            (REPRESENTATION_VERSION,),
        ),
        "provenance_verified": scalar(
            cur,
            """SELECT count(*) FROM episode_representations
               WHERE representation_version=%s
                 AND provenance_status='verified_feature_timestamps'""",
            (REPRESENTATION_VERSION,),
        ),
        "provenance_historical_reconstruction": scalar(
            cur,
            """SELECT count(*) FROM episode_representations
               WHERE representation_version=%s
                 AND provenance_status='historical_reconstruction_unversioned_cluster'""",
            (REPRESENTATION_VERSION,),
        ),
        "market_states_matching": scalar(
            cur,
            """SELECT count(*) FROM episode_market_state ems
               JOIN episode_representations er ON er.episode_id=ems.episode_id
               WHERE er.representation_version=%s AND ems.decision_at=er.as_of""",
            (REPRESENTATION_VERSION,),
        ),
        "outcomes_matching": scalar(
            cur,
            """SELECT count(*) FROM episode_outcomes eo
               JOIN episode_representations er ON er.episode_id=eo.episode_id
               WHERE er.representation_version=%s
                 AND er.provenance_status='verified_feature_timestamps'
                 AND er.embedding IS NOT NULL
                 AND eo.decision_at=er.as_of
                 AND eo.outcome_available_at > eo.decision_at""",
            (REPRESENTATION_VERSION,),
        ),
    }

    violations = {
        "bad_representation_cardinality": scalar(
            cur,
            """SELECT count(*) FROM episode_representations
               WHERE cardinality(source_article_ids) <> source_article_count
                  OR jsonb_array_length(source_membership_intervals) <> source_article_count
                  OR cardinality(embedded_article_ids) <> embedded_article_count
                  OR NOT (embedded_article_ids <@ source_article_ids)""",
        ),
        "verified_membership_not_active_at_cutoff": scalar(
            cur,
            """SELECT count(DISTINCT er.episode_id)
               FROM episode_representations er
               CROSS JOIN LATERAL jsonb_array_elements(
                 er.source_membership_intervals
               ) membership
               WHERE er.provenance_status='verified_feature_timestamps'
                 AND (
                   membership->>'valid_from' IS NULL
                   OR (membership->>'valid_from')::timestamptz > er.source_cutoff
                   OR (
                     membership->>'valid_to' IS NOT NULL
                     AND (membership->>'valid_to')::timestamptz <= er.source_cutoff
                   )
                 )""",
        ),
        "open_membership_disagrees_with_article": scalar(
            cur,
            """SELECT count(*) FROM article_event_memberships membership
               JOIN articles article ON article.id=membership.article_id
               WHERE membership.valid_to IS NULL
                 AND article.event_id IS DISTINCT FROM membership.event_id""",
        ),
        "verified_representation_uses_legacy_event": scalar(
            cur,
            """SELECT count(*) FROM episode_representations er
               JOIN market_episodes me ON me.id=er.episode_id
               JOIN legacy_event_taints taint ON taint.event_id=me.source_event_id
               WHERE er.provenance_status='verified_feature_timestamps'""",
        ),
        "verified_assignment_after_cutoff": scalar(
            cur,
            """SELECT count(*) FROM episode_representations
               WHERE provenance_status='verified_feature_timestamps'
                 AND (source_max_event_assigned_at IS NULL
                      OR source_max_event_assigned_at > source_cutoff)""",
        ),
        "usable_embedding_after_cutoff": scalar(
            cur,
            """SELECT count(*) FROM episode_representations
               WHERE embedding IS NOT NULL
                 AND provenance_status='verified_feature_timestamps'
                 AND (source_max_embedding_generated_at IS NULL
                      OR source_max_embedding_generated_at > source_cutoff)""",
        ),
        "unverified_representation_has_embedding": scalar(
            cur,
            """SELECT count(*) FROM episode_representations
               WHERE provenance_status <> 'verified_feature_timestamps'
                 AND (embedding IS NOT NULL OR embedded_article_count <> 0)""",
        ),
        "stale_market_state_anchor": scalar(
            cur,
            """SELECT count(*) FROM episode_market_state ems
               JOIN episode_representations er ON er.episode_id=ems.episode_id
               WHERE er.representation_version=%s
                 AND ems.decision_at IS DISTINCT FROM er.as_of""",
            (REPRESENTATION_VERSION,),
        ),
        "stale_outcome_anchor": scalar(
            cur,
            """SELECT count(*) FROM episode_outcomes eo
               JOIN episode_representations er ON er.episode_id=eo.episode_id
               WHERE er.representation_version=%s
                 AND eo.decision_at IS DISTINCT FROM er.as_of""",
            (REPRESENTATION_VERSION,),
        ),
        "outcome_available_too_early": scalar(
            cur,
            """SELECT count(*) FROM episode_outcomes
               WHERE outcome_available_at IS NULL
                  OR outcome_available_at <= decision_at""",
        ),
    }

    print("=== couverture PIT ===")
    for name, value in stats.items():
        print(f"{name}: {value}")
    print("=== invariants (tous doivent être 0) ===")
    for name, value in violations.items():
        print(f"{name}: {value}")

    conn.rollback()
    conn.close()
    failed = {name: value for name, value in violations.items() if value}
    if failed:
        raise SystemExit(f"AUDIT KO: {failed}")
    if not stats["representations_usable"] or not stats["outcomes_matching"]:
        raise SystemExit("AUDIT KO: aucun chemin analogues utilisable")
    print("AUDIT OK")


if __name__ == "__main__":
    main()
