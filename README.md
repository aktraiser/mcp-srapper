# mcp-srapper — Market Memory

Couche **mémoire de marché** de Causal Alpha : transforme le corpus de news
(429k articles → 157k events) en **MarketEpisodes** à `t0` propre (sans leakage),
avec état de marché point-in-time et outcomes — exposés ensuite via un MCP scopé
(lecture seule, jamais de SQL générique au modèle).

> Ce repo capture proprement (migrations + scripts versionnés) un travail
> initialement créé **en live sur la DB** sans migration. Il remplace cette dérive.

## Objet central : `MarketEpisode`
Un événement = **un instant `t0`** = l'**onset du burst d'articles** (l'info devient
publique), pas le `first_seen` du cluster (qui, pour un sujet d'anticipation, ancre
sur la rumeur). Ex. earnings Nvidia : `t0` corrigé de +60,9h vers la vraie publication.

`kind` : `point` (burst net) · `saga_primary` (multi-burst à re-segmenter, phase 2) · `recurring` (bruit).

## Schéma (`migrations/001_market_episodes.sql`)
- `market_episodes` — 1 ligne/event, `t0` = onset, provenance `source_event_id`.
- `episode_market_state` — sensor **point-in-time strictement avant `t0`** (FRED + crypto), + label régime.
- `episode_outcomes` — forward returns **leakage-safe** (entrée = 1er close **strictement
  après** `t0` : un event post-clôture US entre le lendemain, jamais au close déjà imprimé).

## Migrations & builders (rôle **en écriture**, ≠ `mcp_ro`)
Les builders **écrivent** et les migrations sont du **DDL** : ils exigent un cred
propriétaire/écriture — **pas** le rôle `mcp_ro` (read-only) que sert le serveur MCP.
```bash
# 1) DDL (hors deploy.sh : nécessite un cred owner, appliqué hors-bande)
psql "$OWNER_DSN" -f migrations/001_market_episodes.sql
psql "$OWNER_DSN" -f migrations/002_outcome_available_at.sql   # ajoute la colonne + index
# 2) Builders (EVENTS_DSN = rôle EN ÉCRITURE ici, jamais commité)
export EVENTS_DSN=postgresql://.../scraping_station
python builders/build_episodes.py       # events -> market_episodes (onset/burst)
python builders/build_market_state.py   # market_state PIT (FRED+crypto)
python builders/build_outcomes.py       # outcomes daily leakage-safe (UPSERT: backfille au re-run)
```
> `002` ajoute seulement la colonne `outcome_available_at` (vide). C'est **le re-run de
> `build_outcomes.py`** (UPSERT) qui la **backfille** sur les lignes préexistantes — sans lui
> elles restent NULL et `find_analogs` les exclut (fail-safe, mais pool vide).

## Limites connues (honnêtes)
- **Donnée marché DB insuffisante** pour valider un *signal* : FRED est **daily et clairsemé**
  (~53 points SP500/6 mois → la moitié des épisodes rejetés par la garde de densité),
  horizon 6 mois, pas d'intraday, pas de cross-section per-stock.
- La **plomberie est validée** (P(up 3j) ≈ 0,52, cohérent ; le 0,26 initial était un
  artefact de couverture, corrigé par la garde dans `build_outcomes.py`).
- Le *vrai* signal (intraday, per-stock, IV/options, abnormal cross-sectionnel) nécessite
  **Alpaca** — hors de ce repo.

## Serveur MCP (lecture seule, `mcp_server/`)
Fail-closed par construction : seuls 3 outils scopés existent, **aucun SQL générique**.
Session DB forcée en **READ ONLY** (défense en profondeur).
```bash
export EVENTS_DSN=postgresql://.../scraping_station
python -m mcp_server.server            # transport streamable-http
```
- `search_episodes(entity, event_type, min_articles, limit)` — lister des épisodes.
- `get_episode(episode_id)` — épisode + market_state PIT + outcome.
- `find_analogs(episode_id, k)` — analogues **past-only** (cosinus ivfflat sur l'embedding
  d'event, pool `n_articles>=3`) + **distribution d'outcomes** (median/p25/p75/prob_up).
  Leakage-safe **strict** : on exige `outcome_available_at(analog) < t0(query)` — l'outcome
  à 3j de l'analogue devait être **réellement connu** avant la requête, pas seulement l'event
  antérieur. Embedding de requête NULL → rejet explicite (`query_embedding_missing`).

## Déploiement (`deploy/`, reproductible)
```bash
bash deploy/deploy.sh    # git pull -> venv -> pip -> systemd -> healthz readiness
```
- **Fail-closed** : sans `MCP_HTTP_TOKEN`, le serveur **refuse de démarrer** (pas d'ouverture
  par défaut) ; `deploy.sh` avorte aussi si le token manque dans `.env`.
- **Bind `127.0.0.1` en dur** : la façade TLS + chemin secret + Host est **Caddy** ; l'env ne
  peut pas exposer le service sur `0.0.0.0`.
- **`/healthz` = readiness** (`SELECT 1`) : renvoie 503 si la DB est injoignable / droits KO
  (pas juste un « je suis vivant » trompeur).
- **Caddy** (`deploy/Caddy.snippet`) : setup **manuel one-time** (chemin secret + `header_up
  Host 127.0.0.1:8788` pour éviter le 421). Hors `deploy.sh` volontairement (édite le
  Caddyfile global). Endpoint réel = `https://<host>/mm-<SECRET>`, `Authorization: Bearer <token>`.

## À venir
- Phase 2 : re-segmentation des sagas (baseline diurne + nouveauté sémantique), embeddings par épisode.
- `find_analogs` : conditionnement par régime (marché similaire), reranker future-relevance.

## Sécurité
- `.env` gitignoré ; aucun secret dans le repo.
- **Auth fail-closed** : bearer constant-time (`hmac.compare_digest`) ; pas de token ⇒ pas de démarrage.
- Serveur bindé **`127.0.0.1`** (frontière Caddy non contournable par l'env).
- Rôle DB **`mcp_ro`** (SELECT-only, 7 tables) + session forcée `READ ONLY` — défense en profondeur.
- Le modèle n'a **jamais** de SQL générique : uniquement des outils de lecture scopés.
- ⚠️ Le credential DB historique en clair (`scraper`, superuser) doit être **rotationné** (action infra) ;
  le MCP n'en dépend plus (il tourne en `mcp_ro`).
