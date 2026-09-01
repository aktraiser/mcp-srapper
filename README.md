# mcp-srapper — Market Memory

Couche **mémoire de marché** de Causal Alpha : transforme le corpus de news
(429k articles → 157k events) en **MarketEpisodes** dotés d'une représentation
sémantique **figée et temporellement tracée**, d'un état de marché et d'outcomes — exposés via
un MCP scopé (lecture seule, jamais de SQL générique au modèle).

> Ce repo capture proprement (migrations + scripts versionnés) un travail
> initialement créé **en live sur la DB** sans migration. Il remplace cette dérive.

## Objet central : `MarketEpisode` + représentation PIT
Trois instants ont des rôles différents :

- `t0` = onset narratif calculé uniquement dans la fenêtre admissible ;
- `source_cutoff` = fin de la fenêtre source figée ;
- `decision_at` / `episode_representations.as_of` = horodatage pris juste après
  l'acquisition du snapshot DB `REPEATABLE READ` qui a réellement vu tous les inputs.

Cette séparation est volontaire : un `statement_timestamp` source peut précéder son
`COMMIT`. Ancrer la décision au snapshot suivant est conservateur et empêche de
prétendre que cette écriture non encore visible était déjà connue au cutoff.

La règle versionnée v2 est : `observed_at = max(published_at, collected_at)`, puis
fenêtre fixe de 6 h. À partir de la migration 003, des triggers horodatent aussi
`event_assigned_at` et `embedding_generated_at` : la fenêtre peut alors être ancrée
sur la disponibilité prouvée du cluster et du vecteur. Seuls les articles admissibles
peuvent définir `t0`, les métadonnées et le centroïde. Leurs IDs exacts sont conservés
et un trigger DB interdit ensuite `UPDATE`/`DELETE` du snapshot.

Les changements d'`event_id` sont enregistrés dans le journal temporel
`article_event_memberships` avec `valid_from/valid_to`. Le freezer relit
l'appartenance qui était active au cutoff, pas l'`event_id` courant : déplacer un
article après le cutoff ne réécrit donc plus le cluster historique.
Les events déjà peuplés lors de la première migration sont inscrits une seule fois
dans `legacy_event_taints` et restent non vérifiables à vie : même si leurs anciens
membres sont déplacés ensuite, ils ne peuvent jamais être promus par erreur.

Le champ `provenance_status` empêche de survendre le backfill historique :

- `verified_feature_timestamps` pour les nouvelles données horodatées par les triggers ;
- `historical_reconstruction_unversioned_cluster` pour les 157k événements anciens,
  dont l'heure historique d'assignation au cluster n'existe tout simplement pas.

Les reconstructions historiques gardent leurs IDs et métadonnées pour l'audit, mais
leur embedding reste volontairement `NULL`. Elles sont exclues de `search_episodes`
et `find_analogs` : une étiquette d'incertitude ne transformerait pas un cluster
historique mutable en donnée leakage-safe.

`kind` : `point` (burst net) · `saga_primary` (multi-burst à re-segmenter, phase 2) · `recurring` (bruit).

## Schéma (`migrations/`)
- `market_episodes` — 1 ligne/event, métadonnées dérivées des seuls articles PIT.
- `episode_representations` — cutoff, IDs et intervalles d'appartenance sources,
  compte d'embeddings, centroïde immuable, méthode/version et statut de provenance ; aucun fallback vers
  `events.embedding_centroid` et aucun centroïde partiel.
- `episode_market_state` — sensor dont la date **et** la vintage `fetched_at` sont
  strictement antérieures à `decision_at`.
- `episode_outcomes` — entrée au premier close XNYS **strictement après**
  `decision_at`, sortie exactement `k` sessions plus tard ; pour le label exposé à
  3 sessions, `outcome_available_at` = max du close réel et de l'heure où sa vintage
  FRED a été collectée.

## Migrations & builders (rôle **en écriture**, ≠ `mcp_ro`)
Les builders **écrivent** et les migrations sont du **DDL** : ils exigent un cred
propriétaire/écriture — **pas** le rôle `mcp_ro` (read-only) que sert le serveur MCP.
```bash
# Environnement builders (séparé du petit runtime serveur MCP)
python -m venv .venv-builders
.venv-builders/bin/pip install -r requirements-builders.txt

# 1) DDL (hors deploy.sh : nécessite un cred owner, appliqué hors-bande)
psql -X -v ON_ERROR_STOP=1 --single-transaction "$OWNER_DSN" \
  -f migrations/001_market_episodes.sql \
  -f migrations/002_outcome_available_at.sql \
  -f migrations/003_point_in_time_representations.sql
# 2) Builders (EVENTS_DSN = rôle EN ÉCRITURE ici, jamais commité)
export EVENTS_DSN=postgresql://.../scraping_station
.venv-builders/bin/python builders/build_episodes.py       # fige fenêtre + centroïde + provenance
.venv-builders/bin/python builders/build_market_state.py   # rebuild atomique à decision_at
.venv-builders/bin/python builders/build_outcomes.py       # rebuild atomique, calendrier XNYS
.venv-builders/bin/python builders/audit_point_in_time.py   # read-only, invariants + couverture
```
> Sur une base déjà en production, procéder en deux phases. D'abord appliquer 003
> seule (migration additive) pour commencer à journaliser les nouveaux memberships ;
> ne pas encore déployer le serveur v2 ni reconstruire state/outcomes. Quand assez
> d'épisodes vérifiés ont dépassé la fenêtre et l'horizon d'outcome, ouvrir une courte
> maintenance : stop → sauvegarde des tables dérivées → `build_episodes` →
> `build_market_state` → `build_outcomes` → audit OK → déploiement v2 → smoke test.
> Si l'audit échoue, ne pas basculer : restaurer les tables dérivées ou laisser la
> mémoire indisponible. Ne jamais servir les reconstructions legacy comme fallback.
> Tant que la représentation ou l'embedding manque, `find_analogs` échoue explicitement.
> Après le premier backfill, l'ancien corpus est auditable mais ne constitue pas encore
> un pool strict : il faut accumuler des épisodes horodatés après 003, ou construire un
> replay causal séparé. L'audit/readiness restent rouges plutôt que de servir un faux prior.

## Limites connues (honnêtes)
- **Donnée marché DB insuffisante** pour valider un *signal* : FRED est daily et
  clairsemé. Le builder strict rejette désormais toute entrée/sortie dont le prix
  manque sur la session XNYS exacte ; il ne saute jamais vers un point ultérieur.
- Le snapshot historique reste explicitement une **reconstruction** conditionnée aux
  `event_id` actuels : aucune migration ne peut inventer l'heure des anciennes
  réaffectations. Il ne fournit donc aucun vecteur au retrieval strict. Les nouvelles
  lignes, elles, disposent des timestamps source requis.
- Une représentation est un snapshot unique à vie par épisode. Le champ
  `representation_version` identifie sa méthode ; une future v3 exige une migration
  explicite des métadonnées/state/outcomes, pas une coexistence partielle trompeuse.
- MVP = première fenêtre détectable de 6 h et une ligne par event. Une vieille rumeur
  suivie d'un burst plusieurs jours plus tard demande encore la segmentation des sagas.
- Le *vrai* signal (intraday, per-stock, IV/options, abnormal cross-sectionnel) nécessite
  **Alpaca** — hors de ce repo.

## Serveur MCP (lecture seule, `mcp_server/`)
Fail-closed par construction : seuls 3 outils scopés existent, **aucun SQL générique**.
Session DB forcée en **READ ONLY** (défense en profondeur).
```bash
export EVENTS_DSN=postgresql://.../scraping_station
python -m mcp_server.server            # transport streamable-http
```
- `search_episodes(entity, event_type, min_articles, limit)` — lister uniquement les
  épisodes dont la provenance temporelle est vérifiée.
- `get_episode(episode_id)` — épisode + représentation + market_state PIT, **sans
  outcome** et uniquement si la provenance est vérifiée. Le label futur et les
  reconstructions legacy ne sont jamais accessibles par ce raccourci.
- `find_analogs(episode_id, k)` — analogues **past-only** (cosinus exact après
  matérialisation du pool PIT filtré, `source_article_count>=3`) + distribution
  d'outcomes. Ce baseline privilégie la correction : un index ANN ne peut pas faire
  disparaître des candidats valides à cause d'un filtre temporel postérieur.
  Filtres stricts : `analog.as_of < query.as_of` et
  `outcome_available_at(analog) < query.as_of`, avec égalité de `decision_at` entre
  représentation et outcome. Embedding absent/incomplet → rejet explicite. La réponse
  expose les statuts de provenance de la requête et des analogues. C'est le seul outil
  agent-facing qui expose `spx_ret_3d`/`dir_3d` ; les autres colonnes d'outcome restent
  des données de recherche internes.

## Déploiement (`deploy/`, reproductible)
```bash
bash deploy/deploy.sh    # git pull -> venv -> pip -> systemd -> healthz readiness
```
- **Fail-closed** : sans `MCP_HTTP_TOKEN`, le serveur **refuse de démarrer** (pas d'ouverture
  par défaut) ; `deploy.sh` avorte aussi si le token manque dans `.env`.
- **Bind `127.0.0.1` en dur** : la façade TLS + chemin secret + Host est **Caddy** ; l'env ne
  peut pas exposer le service sur `0.0.0.0`.
- **`/healthz` = readiness PIT stricte** : renvoie 503 si la migration, les droits ou
  au moins un chemin vérifié représentation → state → outcome au même `decision_at`
  ne sont pas prêts.
- **Caddy** (`deploy/Caddy.snippet`) : setup **manuel one-time** (chemin secret + `header_up
  Host 127.0.0.1:8788` pour éviter le 421). Hors `deploy.sh` volontairement (édite le
  Caddyfile global). Endpoint réel = `https://<host>/mm-<SECRET>`, `Authorization: Bearer <token>`.

## À venir
- Phase 2 : re-segmentation causale des sagas (rumeur → burst) en plusieurs épisodes.
- `find_analogs` : conditionnement par régime (marché similaire), reranker future-relevance.

## Tests locaux
```bash
python -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m pytest -q
```
Les cas couvrent DST hiver/été, jours fériés, fermetures anticipées et exceptionnelle,
égalité exacte au close, horizon en sessions, vintages FRED, backdating, provenance
d'assignation au cluster et invariance aux articles futurs.

## Sécurité
- `.env` gitignoré ; aucun secret dans le repo.
- **Auth fail-closed** : bearer constant-time (`hmac.compare_digest`) ; pas de token ⇒ pas de démarrage.
- Serveur bindé **`127.0.0.1`** (frontière Caddy non contournable par l'env).
- Rôle DB **`mcp_ro`** (SELECT-only, y compris `episode_representations`) + session forcée
  `READ ONLY` — défense en profondeur.
- Le modèle n'a **jamais** de SQL générique : uniquement des outils de lecture scopés.
- ⚠️ Le credential DB historique en clair (`scraper`, superuser) doit être **rotationné** (action infra) ;
  le MCP n'en dépend plus (il tourne en `mcp_ro`).
