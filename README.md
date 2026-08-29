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
- `episode_outcomes` — forward returns **leakage-safe** (entrée = 1er close ≥ `t0`).

## Builders (`builders/`, DSN via env `EVENTS_DSN`)
```bash
export EVENTS_DSN=postgresql://.../scraping_station   # jamais commité
python builders/build_episodes.py       # events -> market_episodes (onset/burst)
python builders/build_market_state.py   # market_state PIT (FRED+crypto)
python builders/build_outcomes.py       # outcomes daily leakage-safe
```

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
  d'event, `t0(analog) < t0(query)`, pool `n_articles>=3`) + **distribution d'outcomes**
  (median/p25/p75/prob_up). Validé : ~0,7s, leakage-safe.

## À venir
- Phase 2 : re-segmentation des sagas (baseline diurne + nouveauté sémantique), embeddings par épisode.
- `find_analogs` : conditionnement par régime (marché similaire), reranker future-relevance.

## Sécurité
- `.env` gitignoré ; aucun secret dans le repo.
- ⚠️ Le credential DB historique en clair doit être **rotationné** (action infra).
- Le modèle n'a **jamais** de SQL générique : uniquement des outils de lecture scopés.
