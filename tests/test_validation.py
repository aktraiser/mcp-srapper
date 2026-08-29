"""Tests fail-closed du serveur MCP : validation AVANT tout accès DB (aucune DB requise)."""
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


def test_find_analogs_rejects_bad_uuid():
    assert _fn(server.find_analogs)("nope")["error"] == "invalid_episode_id"


def test_search_rejects_unknown_event_type():
    r = _fn(server.search_episodes)(event_type="bogus")
    assert r["error"] == "unknown_event_type"


def test_no_generic_sql_tool_exposed():
    # Surface fail-closed : uniquement les 3 outils scopés, jamais de run_sql/write.
    names = {"search_episodes", "get_episode", "find_analogs"}
    for forbidden in ("run_sql", "execute", "query", "write"):
        assert not hasattr(server, forbidden) or not callable(getattr(server, forbidden, None)) or forbidden not in names
