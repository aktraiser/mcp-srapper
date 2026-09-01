"""Tests structurels de la frontière de retrieval (aucune DB requise)."""
from pathlib import Path

from mcp_server import server

ROOT = Path(__file__).resolve().parents[1]


def test_analog_query_uses_only_frozen_representations():
    sql = " ".join(server.FIND_ANALOGS_SQL.lower().split())
    assert "episode_representations" in sql
    assert "aer.representation_version = %s" in sql
    assert "aer.provenance_status = 'verified_feature_timestamps'" in sql
    assert "aer.embedded_article_count = aer.source_article_count" in sql
    assert "embedding_centroid" not in sql
    assert " join events " not in sql
    assert "aer.as_of < %s" in sql
    assert "ao.decision_at = aer.as_of" in sql
    assert "ao.outcome_available_at < %s" in sql


def test_migration_records_provenance_and_freeze_invariants():
    migration = (ROOT / "migrations/003_point_in_time_representations.sql").read_text()
    for required in (
        "source_article_ids",
        "source_article_count",
        "source_membership_intervals",
        "embedded_article_ids",
        "embedded_article_count",
        "source_cutoff <= as_of",
        "source_max_observed_at <= source_cutoff",
        "source_max_event_assigned_at <= source_cutoff",
        "source_max_embedding_generated_at <= source_cutoff",
        "cardinality(source_article_ids) = source_article_count",
        "cardinality(embedded_article_ids) = embedded_article_count",
        "embedding IS NULL OR embedded_article_count = source_article_count",
        "episode_representations_immutable",
        "article_event_memberships",
        "articles_event_membership_history",
        "article_event_memberships_protected",
        "legacy_event_taints",
        "003_legacy_event_taints_seeded",
    ):
        assert required in migration


def test_builder_has_no_mutable_event_metadata_or_centroid_dependency():
    builder = (ROOT / "builders/build_episodes.py").read_text()
    assert "FROM events" not in builder
    assert "embedding_centroid" not in builder
    assert "ON CONFLICT (episode_id) DO NOTHING" in builder
    assert "FROM article_event_memberships" in builder
    assert "SELECT event_id FROM legacy_event_taints" in builder
    assert "SELECT 1 FROM articles LIMIT 0" in builder
    assert "SELECT clock_timestamp()" in builder


def test_historical_reconstruction_never_gets_an_embedding():
    migration = (ROOT / "migrations/003_point_in_time_representations.sql").read_text()
    builder = (ROOT / "builders/build_episodes.py").read_text()
    assert "OR (embedding IS NULL AND embedded_article_count = 0)" in migration
    assert "s.provenance_status = 'verified_feature_timestamps'" in builder


def test_agent_facing_get_episode_never_returns_future_outcome():
    import inspect

    source = inspect.getsource(getattr(server.get_episode, "fn", server.get_episode))
    assert "episode_outcomes" not in source
    assert '"outcome"' not in source
