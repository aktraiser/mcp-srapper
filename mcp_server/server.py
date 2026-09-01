#!/usr/bin/env python3
"""Serveur MCP — Market Memory (LECTURE SEULE, outils scopés).

Fail-closed par construction : seuls les 3 outils déclarés existent. Aucun SQL
générique n'est exposé au modèle. La session DB est forcée en READ ONLY (défense
en profondeur : même un bug ne peut pas écrire).

Outils :
  - search_episodes : lister des épisodes (par entité / type / n_articles).
  - get_episode     : un épisode + sa représentation + son market_state PIT,
                      sans label futur.
  - find_analogs    : analogues PAST-ONLY sur représentations immuables + distribution
                      d'outcomes. Cutoff = instant où la requête était disponible.

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

_conn = None

# Types d'events connus (allowlist -> pas d'injection de valeur arbitraire).
EVENT_TYPES = {
    "announcement", "earnings", "acquisition", "regulation", "decision",
    "incident", "trend", "partnership", "product", "legal", "forecast",
}
REPRESENTATION_VERSION = "pit-v2-observed-window-6h"

# Gardée comme constante pour rendre la frontière anti-fuite testable : cette requête
# ne doit JAMAIS rejoindre `events` ni son centroïde mutable.
FIND_ANALOGS_SQL = """
    WITH eligible AS MATERIALIZED (
      SELECT a.id, a.t0, aer.as_of AS decision_at, a.event_type,
             aer.source_article_count AS n_articles, a.signature,
             aer.provenance_status, ao.spx_ret_3d, ao.dir_3d, aer.embedding
      FROM market_episodes a
      JOIN episode_representations aer
        ON aer.episode_id = a.id
       AND aer.embedding IS NOT NULL
       AND aer.embedded_article_count = aer.source_article_count
       AND aer.representation_version = %s
       AND aer.provenance_status = 'verified_feature_timestamps'
      JOIN episode_outcomes ao ON ao.episode_id = a.id
      WHERE a.id <> %s
        AND a.kind <> 'recurring'
        AND NOT EXISTS (
          SELECT 1 FROM legacy_event_taints taint
          WHERE taint.event_id = a.source_event_id
        )
        AND aer.source_article_count >= 3
        AND aer.as_of < %s
        AND ao.decision_at = aer.as_of
        AND ao.outcome_available_at IS NOT NULL
        AND ao.outcome_available_at < %s
    )
    SELECT id, t0, decision_at, event_type, n_articles, signature,
           provenance_status,
           spx_ret_3d, dir_3d
    FROM eligible
    ORDER BY embedding <=> %s::vector
    LIMIT %s
"""


def _db():
    """Connexion cachée, session forcée en READ ONLY."""
    global _conn
    if _conn is not None and _conn.closed == 0:
        return _conn
    dsn = os.getenv("EVENTS_DSN", "").strip()
    if not dsn:
        raise RuntimeError("EVENTS_DSN manque")
    _conn = psycopg2.connect(dsn, connect_timeout=10)
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
    clauses = [
        "me.kind <> 'recurring'",
        "er.provenance_status = 'verified_feature_timestamps'",
        "NOT EXISTS (SELECT 1 FROM legacy_event_taints taint "
        "WHERE taint.event_id = me.source_event_id)",
        "er.source_article_count >= %s",
    ]
    params: list = [min_articles]
    if entity:
        clauses.append("(me.signature ILIKE %s OR array_to_string(me.main_entities, ' ') ILIKE %s)")
        params += [f"%{entity[:80]}%", f"%{entity[:80]}%"]
    if event_type:
        if event_type not in EVENT_TYPES:
            return {"error": "unknown_event_type", "allowed": sorted(EVENT_TYPES)}
        clauses.append("me.event_type = %s")
        params.append(event_type)
    params.append(limit)
    with _db().cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(f"""
            SELECT me.id, me.t0, er.as_of AS decision_at, me.kind, me.event_type,
                   er.source_article_count AS n_articles, me.main_entities, me.signature,
                   er.provenance_status
            FROM market_episodes me
            JOIN episode_representations er
              ON er.episode_id = me.id AND er.representation_version = %s
            WHERE {' AND '.join(clauses)}
            ORDER BY er.as_of DESC LIMIT %s""", [REPRESENTATION_VERSION, *params])
        return {"episodes": [dict(r) for r in cur.fetchall()]}


@mcp.tool()
def get_episode(episode_id: str) -> dict:
    """Métadonnées et market_state connus à la décision, sans label futur.

    Les outcomes ne sont volontairement pas exposés ici : le seul chemin agentique
    qui peut en lire est :func:`find_analogs`, avec son cutoff interne past-only.
    """
    try:
        eid = _valid_uuid(episode_id)
    except ValueError:
        return {"error": "invalid_episode_id"}
    with _db().cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM market_episodes WHERE id = %s", [eid])
        ep = cur.fetchone()
        if not ep:
            return {"error": "not_found"}
        cur.execute("""
            SELECT first_observed_at, source_cutoff, as_of AS decision_at,
                   source_article_count,
                   embedded_article_count, source_max_published_at,
                   source_max_observed_at, source_max_event_assigned_at,
                   source_max_embedding_generated_at, method,
                   representation_version,
                   provenance_status,
                   EXISTS (
                     SELECT 1 FROM legacy_event_taints taint
                     JOIN market_episodes me ON me.source_event_id = taint.event_id
                     WHERE me.id = episode_representations.episode_id
                   ) AS legacy_event_tainted,
                   (embedding IS NOT NULL) AS has_embedding
            FROM episode_representations
            WHERE episode_id = %s AND representation_version = %s
        """, [eid, REPRESENTATION_VERSION])
        representation = cur.fetchone()
        if not representation:
            return {"error": "representation_missing"}
        if (
            representation["provenance_status"] != "verified_feature_timestamps"
            or representation["legacy_event_tainted"]
        ):
            return {
                "error": "query_temporal_provenance_unverified",
                "provenance_status": representation["provenance_status"],
            }
        cur.execute("""
            SELECT ems.state, ems.coverage_ok, ems.decision_at
            FROM episode_market_state ems
            JOIN episode_representations er
              ON er.episode_id = ems.episode_id AND er.representation_version = %s
            WHERE ems.episode_id = %s AND ems.decision_at = er.as_of
        """, [REPRESENTATION_VERSION, eid])
        state = cur.fetchone()
    return {"episode": dict(ep),
            "representation": dict(representation),
            "market_state": dict(state) if state else None}


@mcp.tool()
def find_analogs(episode_id: str, k: int = 46) -> dict:
    """Analogues d'un épisode sur son centroïde PIT immuable + outcomes réalisés.

    Deux cutoffs stricts : représentation de l'analogue et outcome à trois sessions
    doivent être antérieurs à `as_of(query)`. Aucun fallback vers le centroïde mutable
    de `events`. Un embedding absent est rejeté explicitement.
    """
    try:
        qid = _valid_uuid(episode_id)
    except ValueError:
        return {"error": "invalid_episode_id"}
    k = max(1, min(int(k), 100))
    with _db().cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT me.t0, er.as_of, er.embedding, er.provenance_status,
                   er.source_article_count, er.embedded_article_count,
                   EXISTS (
                     SELECT 1 FROM legacy_event_taints taint
                     WHERE taint.event_id = me.source_event_id
                   ) AS legacy_event_tainted,
                   (er.episode_id IS NOT NULL) AS has_representation,
                   (er.embedding IS NOT NULL) AS has_emb
            FROM market_episodes me
            LEFT JOIN episode_representations er
              ON er.episode_id = me.id AND er.representation_version = %s
            WHERE me.id = %s""", [REPRESENTATION_VERSION, qid])
        q = cur.fetchone()
        if not q:
            return {"error": "not_found", "analogs": []}
        if not q["has_representation"]:
            return {"error": "representation_missing", "analogs": []}
        if q["provenance_status"] != "verified_feature_timestamps" or q["legacy_event_tainted"]:
            return {"error": "query_temporal_provenance_unverified", "analogs": []}
        if not q["has_emb"]:
            error = (
                "query_embedding_incomplete"
                if q["embedded_article_count"] < q["source_article_count"]
                else "query_embedding_missing"
            )
            return {"error": error, "analogs": []}
        cur.execute(
            FIND_ANALOGS_SQL,
            [REPRESENTATION_VERSION, qid, q["as_of"], q["as_of"], q["embedding"], k],
        )
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
    statuses = sorted({row["provenance_status"] for row in rows})
    return {
        "distribution": dist,
        "analogs": rows,
        "query_provenance_status": q["provenance_status"],
        "analog_provenance_statuses": statuses,
        "temporal_provenance": "verified_feature_timestamps",
    }


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
                        # Détecte aussi une migration 003 oubliée ou un GRANT mcp_ro
                        # manquant, ce qu'un simple `SELECT 1` ne voyait pas.
                        cur.execute("""
                            SELECT EXISTS (
                              SELECT 1
                            FROM episode_representations er
                            JOIN episode_market_state ems
                              ON ems.episode_id = er.episode_id
                             AND ems.decision_at = er.as_of
                            JOIN episode_outcomes eo
                              ON eo.episode_id = er.episode_id
                             AND eo.decision_at = er.as_of
                            WHERE er.representation_version = %s
                              AND er.provenance_status = 'verified_feature_timestamps'
                              AND er.embedding IS NOT NULL
                              AND er.embedded_article_count = er.source_article_count
                              AND eo.outcome_available_at > eo.decision_at
                              AND NOT EXISTS (
                                SELECT 1 FROM legacy_event_taints taint
                                JOIN market_episodes me
                                  ON me.source_event_id = taint.event_id
                                WHERE me.id = er.episode_id
                              )
                            LIMIT 1
                            )
                        """, [REPRESENTATION_VERSION])
                        if not cur.fetchone()[0]:
                            raise RuntimeError("point_in_time_backfill_incomplete")
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
    if not os.getenv("EVENTS_DSN", "").strip():
        sys.exit("REFUS: EVENTS_DSN vide/absent — aucune base read-only configurée.")
    app = _wrap(mcp.streamable_http_app(), token)
    uvicorn.run(app, host=host, port=port, log_level="info")
