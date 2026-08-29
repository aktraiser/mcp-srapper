#!/usr/bin/env python3
"""Serveur MCP — Market Memory (LECTURE SEULE, outils scopés).

Fail-closed par construction : seuls les 3 outils déclarés existent. Aucun SQL
générique n'est exposé au modèle. La session DB est forcée en READ ONLY (défense
en profondeur : même un bug ne peut pas écrire).

Outils :
  - search_episodes : lister des épisodes (par entité / type / n_articles).
  - get_episode     : un épisode + son market_state PIT + son outcome.
  - find_analogs    : analogues PAST-ONLY (cosinus sur embedding d'event) + distribution
                      d'outcomes. Leakage-safe : t0(analog) < t0(query).

Lancement : EVENTS_DSN=... python -m mcp_server.server   (transport streamable-http)
"""
from __future__ import annotations
import hmac
import os
import statistics as st
from uuid import UUID

import psycopg2
from psycopg2.extras import RealDictCursor
from mcp.server.fastmcp import FastMCP
from starlette.responses import JSONResponse, PlainTextResponse

DSN = os.environ["EVENTS_DSN"]
_conn = None

# Types d'events connus (allowlist -> pas d'injection de valeur arbitraire).
EVENT_TYPES = {
    "announcement", "earnings", "acquisition", "regulation", "decision",
    "incident", "trend", "partnership", "product", "legal",
}


def _db():
    """Connexion cachée, session forcée en READ ONLY."""
    global _conn
    if _conn is not None and _conn.closed == 0:
        return _conn
    _conn = psycopg2.connect(DSN, connect_timeout=10)
    _conn.autocommit = True
    with _conn.cursor() as cur:
        cur.execute("SET default_transaction_read_only = on")
    return _conn


def _valid_uuid(v: str) -> str:
    return str(UUID(str(v)))  # lève ValueError si invalide


mcp = FastMCP("market-memory")


@mcp.tool()
def search_episodes(entity: str = "", event_type: str = "", min_articles: int = 3,
                    limit: int = 20) -> dict:
    """Lister des MarketEpisodes récents. `entity` filtre sur la signature/entités,
    `event_type` doit appartenir à la taxonomie connue, `min_articles` borne le bruit."""
    limit = max(1, min(int(limit), 50))
    min_articles = max(1, min(int(min_articles), 1000))
    clauses = ["kind <> 'recurring'", "n_articles >= %s"]
    params: list = [min_articles]
    if entity:
        clauses.append("(signature ILIKE %s OR array_to_string(main_entities, ' ') ILIKE %s)")
        params += [f"%{entity[:80]}%", f"%{entity[:80]}%"]
    if event_type:
        if event_type not in EVENT_TYPES:
            return {"error": "unknown_event_type", "allowed": sorted(EVENT_TYPES)}
        clauses.append("event_type = %s")
        params.append(event_type)
    params.append(limit)
    with _db().cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(f"""
            SELECT id, t0, kind, event_type, n_articles, main_entities, signature
            FROM market_episodes WHERE {' AND '.join(clauses)}
            ORDER BY t0 DESC LIMIT %s""", params)
        return {"episodes": [dict(r) for r in cur.fetchall()]}


@mcp.tool()
def get_episode(episode_id: str) -> dict:
    """Un épisode complet : métadonnées + market_state point-in-time + outcome."""
    try:
        eid = _valid_uuid(episode_id)
    except ValueError:
        return {"error": "invalid_episode_id"}
    with _db().cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM market_episodes WHERE id = %s", [eid])
        ep = cur.fetchone()
        if not ep:
            return {"error": "not_found"}
        cur.execute("SELECT state, coverage_ok FROM episode_market_state WHERE episode_id = %s", [eid])
        state = cur.fetchone()
        cur.execute("""SELECT spx_ret_1d, spx_ret_3d, spx_ret_7d, vix_chg_1d, oil_chg_1d, dir_3d
                       FROM episode_outcomes WHERE episode_id = %s""", [eid])
        out = cur.fetchone()
    return {"episode": dict(ep),
            "market_state": dict(state) if state else None,
            "outcome": dict(out) if out else None}


@mcp.tool()
def find_analogs(episode_id: str, k: int = 46) -> dict:
    """Analogues d'un épisode (cosinus sur l'embedding d'event) + distribution des
    outcomes réalisés. LEAKAGE-SAFE : on exige que l'outcome à 3j de l'analogue ait été
    RÉELLEMENT connu avant la requête (`outcome_available_at < t0(query)`) — pas seulement
    que l'event soit antérieur. Un embedding de requête NULL est rejeté (pas d'analogues
    arbitraires). Pool = vrais events (n_articles>=3)."""
    try:
        qid = _valid_uuid(episode_id)
    except ValueError:
        return {"error": "invalid_episode_id"}
    k = max(1, min(int(k), 100))
    with _db().cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT me.t0, (qe.embedding_centroid IS NOT NULL) AS has_emb
            FROM market_episodes me JOIN events qe ON qe.id = me.source_event_id
            WHERE me.id = %s""", [qid])
        q = cur.fetchone()
        if not q:
            return {"error": "not_found", "analogs": []}
        if not q["has_emb"]:
            return {"error": "query_embedding_missing", "analogs": []}
        cur.execute("""
            SELECT a.id, a.t0, a.event_type, a.n_articles, a.signature,
                   ao.spx_ret_3d, ao.dir_3d
            FROM market_episodes a
            JOIN events ae ON ae.id = a.source_event_id AND ae.embedding_centroid IS NOT NULL
            JOIN episode_outcomes ao ON ao.episode_id = a.id
            WHERE a.id <> %s AND a.n_articles >= 3
              AND ao.outcome_available_at IS NOT NULL
              AND ao.outcome_available_at < %s
            ORDER BY ae.embedding_centroid <=> (
              SELECT qe.embedding_centroid FROM market_episodes me
              JOIN events qe ON qe.id = me.source_event_id WHERE me.id = %s)
            LIMIT %s""", [qid, q["t0"], qid, k])
        rows = [dict(r) for r in cur.fetchall()]
    if not rows:
        return {"error": "no_leakage_safe_analogs", "analogs": []}
    r3 = sorted(float(r["spx_ret_3d"]) for r in rows if r["spx_ret_3d"] is not None)
    dist = None
    if r3:
        dist = {
            "n": len(r3),
            "median_3d": round(st.median(r3), 3),
            "p25_3d": round(r3[len(r3) // 4], 3),
            "p75_3d": round(r3[3 * len(r3) // 4], 3),
            "prob_up_3d": round(sum(x > 0 for x in r3) / len(r3), 3),
        }
    return {"distribution": dist, "analogs": rows}


def _wrap(app, token: str):
    """ASGI wrapper : /healthz public + auth bearer (constant-time) sur le reste.
    Défense en profondeur : ne dépend pas uniquement de la config Caddy (non versionnée)."""
    async def wrapped(scope, receive, send):
        if scope["type"] == "http":
            if scope.get("path", "") == "/healthz":
                # Readiness, pas juste liveness : la DB étant connectée paresseusement,
                # on vérifie qu'elle répond ET que le rôle a bien le SELECT (sinon 503).
                try:
                    with _db().cursor() as cur:
                        cur.execute("SELECT 1")
                        cur.fetchone()
                    await JSONResponse({"status": "ok", "server": "market-memory", "db": "ok"})(scope, receive, send)
                except Exception as e:
                    await JSONResponse(
                        {"status": "degraded", "server": "market-memory", "db": type(e).__name__},
                        status_code=503,
                    )(scope, receive, send)
                return
            if token:
                headers = {k.decode().lower(): v.decode() for k, v in scope.get("headers", [])}
                given = headers.get("authorization", "")
                if not hmac.compare_digest(given, f"Bearer {token}"):
                    await PlainTextResponse("unauthorized", status_code=401)(scope, receive, send)
                    return
        await app(scope, receive, send)
    return wrapped


if __name__ == "__main__":
    import sys
    import uvicorn
    # Bind localhost EN DUR : la façade (TLS + chemin secret) est Caddy ; on ne laisse
    # pas l'env exposer le service sur 0.0.0.0 et contourner cette frontière.
    host = "127.0.0.1"
    port = int(os.getenv("MCP_HTTP_PORT", "8788"))
    token = os.getenv("MCP_HTTP_TOKEN", "").strip()
    # FAIL-CLOSED : pas de token = pas de démarrage (sinon MCP ouvert par défaut).
    if not token:
        sys.exit("REFUS: MCP_HTTP_TOKEN vide/absent — le serveur refuse de démarrer sans auth.")
    app = _wrap(mcp.streamable_http_app(), token)
    uvicorn.run(app, host=host, port=port, log_level="info")
