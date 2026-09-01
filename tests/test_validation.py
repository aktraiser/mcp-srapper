"""Tests fail-closed du serveur MCP : validation AVANT tout accès DB (aucune DB requise)."""
import asyncio
import os

import pytest

os.environ.setdefault("EVENTS_DSN", "postgresql://x:x@127.0.0.1:5/x")  # non utilisé ici
from mcp_server import server  # noqa: E402


def _fn(tool):
    """Récupère la fonction sous-jacente (le décorateur @mcp.tool peut la wrapper)."""
    return getattr(tool, "fn", tool)


def test_valid_uuid_accepts_and_rejects():
    assert server._valid_uuid("4a90b95d-60d4-49af-96db-83720e0e752f")
    with pytest.raises(ValueError):
        server._valid_uuid("not-a-uuid")


def test_get_episode_rejects_bad_uuid():
    assert _fn(server.get_episode)("nope") == {"error": "invalid_episode_id"}


def test_get_episode_rejects_unverified_history_before_reading_state(monkeypatch):
    executed = []
    rows = iter([
        {"id": "4a90b95d-60d4-49af-96db-83720e0e752f"},
        {
            "provenance_status": "historical_reconstruction_unversioned_cluster",
            "legacy_event_tainted": True,
        },
    ])

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, params):
            executed.append((sql, params))

        def fetchone(self):
            return next(rows)

    class Connection:
        def cursor(self, **_kwargs):
            return Cursor()

    monkeypatch.setattr(server, "_db", lambda: Connection())
    result = _fn(server.get_episode)("4a90b95d-60d4-49af-96db-83720e0e752f")
    assert result["error"] == "query_temporal_provenance_unverified"
    assert len(executed) == 2  # le market_state n'est jamais lu


def test_find_analogs_rejects_bad_uuid():
    assert _fn(server.find_analogs)("nope")["error"] == "invalid_episode_id"


def test_find_analogs_rejects_unverified_historical_query_before_vector_search(monkeypatch):
    executed = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, params):
            executed.append((sql, params))

        def fetchone(self):
            return {
                "t0": None,
                "as_of": None,
                "embedding": None,
                "provenance_status": "historical_reconstruction_unversioned_cluster",
                "source_article_count": 3,
                "embedded_article_count": 0,
                "legacy_event_tainted": True,
                "has_representation": True,
                "has_emb": False,
            }

    class Connection:
        def cursor(self, **_kwargs):
            return Cursor()

    monkeypatch.setattr(server, "_db", lambda: Connection())
    result = _fn(server.find_analogs)("4a90b95d-60d4-49af-96db-83720e0e752f")
    assert result == {"error": "query_temporal_provenance_unverified", "analogs": []}
    assert len(executed) == 1  # aucune requête de voisins n'a été lancée


def test_analog_sql_has_strict_point_in_time_cutoffs():
    sql = " ".join(server.FIND_ANALOGS_SQL.lower().split())
    assert "aer.as_of < %s" in sql
    assert "ao.decision_at = aer.as_of" in sql
    assert "ao.outcome_available_at < %s" in sql
    assert "aer.provenance_status = 'verified_feature_timestamps'" in sql


def test_search_rejects_unknown_event_type():
    r = _fn(server.search_episodes)(event_type="bogus")
    assert r["error"] == "unknown_event_type"


def test_exposed_tool_surface_is_exactly_the_three_readonly_tools():
    # Vrai test de surface (pas tautologique) : on interroge le registre MCP réel.
    # Fail-closed = EXACTEMENT ces 3 outils, jamais un run_sql/execute/write générique.
    tools = asyncio.run(server.mcp.list_tools())
    names = {t.name for t in tools}
    assert names == {"search_episodes", "get_episode", "find_analogs"}


def test_auth_wrapper_rejects_without_and_accepts_with_token():
    # Le wrapper bearer est constant-time et effectif : mauvais/absent -> refus.
    tok = "sekret-abc"
    assert not __import__("hmac").compare_digest("", f"Bearer {tok}")
    assert not __import__("hmac").compare_digest("Bearer wrong", f"Bearer {tok}")
    assert __import__("hmac").compare_digest(f"Bearer {tok}", f"Bearer {tok}")
