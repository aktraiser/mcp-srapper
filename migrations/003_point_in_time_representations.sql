-- 003 — Représentation d'épisode figée + provenance temporelle explicite.
--
-- Un `events.embedding_centroid` évolue quand de nouveaux articles rejoignent le
-- cluster. Il ne peut donc pas servir de mémoire historique. Cette table fige une
-- représentation construite uniquement avec les articles admissibles pendant une
-- fenêtre de six heures. Les nouvelles lignes disposent de timestamps de features ;
-- le backfill ancien est étiqueté honnêtement comme reconstruction.
--
-- La représentation est volontairement séparée de `market_episodes` : le MCP peut
-- exposer les métadonnées sans renvoyer un vecteur de 1536 nombres dans `get_episode`.

-- L'ancien schéma ne mémorisait ni l'heure d'assignation au cluster, ni celle de
-- génération de l'embedding. On ne fabrique surtout pas de faux backfill : les lignes
-- historiques gardent ces timestamps à NULL, leur représentation n'a pas de vecteur
-- et elle est marquée comme reconstruction. Le trigger rend les nouveaux articles
-- auditables à partir de cette migration.
ALTER TABLE articles
  ADD COLUMN IF NOT EXISTS event_assigned_at timestamptz;
ALTER TABLE articles
  ADD COLUMN IF NOT EXISTS embedding_generated_at timestamptz;

-- Les events qui possédaient déjà des membres lors de la première application sont
-- irrémédiablement non prouvables. On les marque UNE seule fois, sans leur inventer
-- de valid_from. Le marker empêche qu'un re-run de 003 ne contamine les nouveaux events.
CREATE TABLE IF NOT EXISTS market_memory_migration_markers (
  name        text PRIMARY KEY,
  recorded_at timestamptz NOT NULL DEFAULT statement_timestamp()
);
CREATE TABLE IF NOT EXISTS legacy_event_taints (
  event_id    uuid PRIMARY KEY,
  reason      text NOT NULL,
  detected_at timestamptz NOT NULL DEFAULT statement_timestamp()
);
DO $seed_legacy_taints$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM market_memory_migration_markers
    WHERE name = '003_legacy_event_taints_seeded'
  ) THEN
    INSERT INTO legacy_event_taints(event_id, reason)
    SELECT DISTINCT event_id, 'membership_predates_003'
    FROM articles
    WHERE event_id IS NOT NULL
    ON CONFLICT (event_id) DO NOTHING;

    INSERT INTO market_memory_migration_markers(name)
    VALUES ('003_legacy_event_taints_seeded');
  END IF;
END;
$seed_legacy_taints$;

CREATE OR REPLACE FUNCTION stamp_article_feature_availability()
RETURNS trigger LANGUAGE plpgsql AS $availability$
BEGIN
  IF TG_OP = 'INSERT' THEN
    IF NEW.embedding IS NOT NULL THEN
      -- Disponibilité dans CETTE base : un appelant ne peut pas antidater la preuve.
      NEW.embedding_generated_at = statement_timestamp();
    ELSE
      NEW.embedding_generated_at = NULL;
    END IF;
    IF NEW.event_id IS NOT NULL THEN
      NEW.event_assigned_at = statement_timestamp();
    ELSE
      NEW.event_assigned_at = NULL;
    END IF;
  ELSE
    IF OLD.embedding IS DISTINCT FROM NEW.embedding THEN
      NEW.embedding_generated_at = CASE
        WHEN NEW.embedding IS NULL THEN NULL
        ELSE statement_timestamp()
      END;
    ELSIF OLD.embedding_generated_at IS DISTINCT FROM NEW.embedding_generated_at THEN
      RAISE EXCEPTION 'embedding_generated_at est géré par la base';
    END IF;
    IF OLD.event_id IS DISTINCT FROM NEW.event_id THEN
      NEW.event_assigned_at = CASE
        WHEN NEW.event_id IS NULL THEN NULL
        ELSE statement_timestamp()
      END;
    ELSIF OLD.event_assigned_at IS DISTINCT FROM NEW.event_assigned_at THEN
      RAISE EXCEPTION 'event_assigned_at est géré par la base';
    END IF;
  END IF;
  RETURN NEW;
END;
$availability$;

DROP TRIGGER IF EXISTS articles_feature_availability
  ON articles;
CREATE TRIGGER articles_feature_availability
BEFORE INSERT OR UPDATE OF embedding, event_id, embedding_generated_at, event_assigned_at
ON articles
FOR EACH ROW EXECUTE FUNCTION stamp_article_feature_availability();

-- Ledger bitemporel des appartenances. On ne backfille PAS les lignes existantes :
-- leur première affectation historique est inconnue. À partir de 003, une réaffectation
-- ferme l'intervalle précédent au lieu de réécrire silencieusement le passé.
CREATE TABLE IF NOT EXISTS article_event_memberships (
  article_id  uuid NOT NULL REFERENCES articles(id) ON DELETE RESTRICT,
  event_id    uuid NOT NULL,
  valid_from  timestamptz NOT NULL,
  valid_to    timestamptz,
  recorded_at timestamptz NOT NULL DEFAULT statement_timestamp(),
  PRIMARY KEY (article_id, valid_from),
  CHECK (valid_to IS NULL OR valid_to > valid_from)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_article_event_memberships_open
  ON article_event_memberships (article_id) WHERE valid_to IS NULL;
CREATE INDEX IF NOT EXISTS idx_article_event_memberships_event_time
  ON article_event_memberships (event_id, valid_from, valid_to);

-- Le writer applicatif passe exclusivement par articles.event_id. Le journal ne peut
-- être inséré/fermé que par le trigger imbriqué ci-dessous, jamais édité directement.
CREATE OR REPLACE FUNCTION protect_article_event_membership_history()
RETURNS trigger LANGUAGE plpgsql AS $protect_membership$
BEGIN
  IF pg_trigger_depth() < 2 THEN
    RAISE EXCEPTION 'article_event_memberships est géré par le trigger articles.event_id';
  END IF;
  IF TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'suppression interdite dans article_event_memberships';
  END IF;
  IF TG_OP = 'UPDATE' AND (
    OLD.article_id IS DISTINCT FROM NEW.article_id
    OR OLD.event_id IS DISTINCT FROM NEW.event_id
    OR OLD.valid_from IS DISTINCT FROM NEW.valid_from
    OR OLD.recorded_at IS DISTINCT FROM NEW.recorded_at
    OR OLD.valid_to IS NOT NULL
    OR NEW.valid_to IS NULL
    OR NEW.valid_to IS DISTINCT FROM statement_timestamp()
  ) THEN
    RAISE EXCEPTION 'seule la clôture monotone du membership courant est permise';
  END IF;
  RETURN NEW;
END;
$protect_membership$;

DROP TRIGGER IF EXISTS article_event_memberships_protected
  ON article_event_memberships;
CREATE TRIGGER article_event_memberships_protected
BEFORE INSERT OR UPDATE OR DELETE ON article_event_memberships
FOR EACH ROW EXECUTE FUNCTION protect_article_event_membership_history();

CREATE OR REPLACE FUNCTION track_article_event_membership()
RETURNS trigger LANGUAGE plpgsql AS $membership$
DECLARE
  changed_at timestamptz := statement_timestamp();
BEGIN
  IF TG_OP = 'INSERT' THEN
    IF NEW.event_id IS NOT NULL THEN
      INSERT INTO article_event_memberships(article_id, event_id, valid_from)
      VALUES (NEW.id, NEW.event_id, changed_at);
    END IF;
  ELSIF OLD.event_id IS DISTINCT FROM NEW.event_id THEN
    IF OLD.event_id IS NOT NULL THEN
      UPDATE article_event_memberships
      SET valid_to = changed_at
      WHERE article_id = NEW.id AND event_id = OLD.event_id AND valid_to IS NULL;
      -- Zéro ligne = appartenance legacy antérieure à 003 : on ne lui invente
      -- surtout pas un valid_from rétrospectif.
    END IF;
    IF NEW.event_id IS NOT NULL THEN
      INSERT INTO article_event_memberships(article_id, event_id, valid_from)
      VALUES (NEW.id, NEW.event_id, changed_at);
    END IF;
  END IF;
  RETURN NEW;
END;
$membership$;

DROP TRIGGER IF EXISTS articles_event_membership_history ON articles;
CREATE TRIGGER articles_event_membership_history
AFTER INSERT OR UPDATE OF event_id ON articles
FOR EACH ROW EXECUTE FUNCTION track_article_event_membership();

CREATE TABLE IF NOT EXISTS episode_representations (
  episode_id                uuid PRIMARY KEY
                            REFERENCES market_episodes(id) ON DELETE RESTRICT,
  first_observed_at         timestamptz NOT NULL,
  source_cutoff             timestamptz NOT NULL,
  as_of                     timestamptz NOT NULL,
  embedding                 vector(1536),
  source_article_ids        uuid[] NOT NULL,
  source_article_count      int NOT NULL CHECK (source_article_count > 0),
  source_membership_intervals jsonb NOT NULL,
  embedded_article_ids      uuid[] NOT NULL,
  embedded_article_count    int NOT NULL CHECK (
                              embedded_article_count >= 0
                              AND embedded_article_count <= source_article_count
                            ),
  source_max_published_at   timestamptz NOT NULL,
  source_max_observed_at    timestamptz NOT NULL,
  source_max_event_assigned_at timestamptz,
  source_max_embedding_generated_at timestamptz,
  method                    text NOT NULL,
  representation_version   text NOT NULL DEFAULT 'pit-v2-observed-window-6h',
  provenance_status         text NOT NULL CHECK (provenance_status IN (
                              'verified_feature_timestamps',
                              'historical_reconstruction_unversioned_cluster'
                            )),
  built_at                  timestamptz NOT NULL DEFAULT now(),
  CHECK (first_observed_at <= source_max_observed_at),
  CHECK (first_observed_at <= source_cutoff),
  CHECK (source_cutoff <= as_of),
  CHECK (source_max_observed_at <= source_cutoff),
  CHECK (source_max_published_at <= source_cutoff),
  CHECK (cardinality(source_article_ids) = source_article_count),
  CHECK (jsonb_typeof(source_membership_intervals) = 'array'),
  CHECK (jsonb_array_length(source_membership_intervals) = source_article_count),
  CHECK (cardinality(embedded_article_ids) = embedded_article_count),
  CHECK (embedded_article_ids <@ source_article_ids),
  CHECK (embedding IS NULL OR embedded_article_count = source_article_count),
  CHECK (
    provenance_status <> 'verified_feature_timestamps'
    OR (
      source_max_event_assigned_at IS NOT NULL
      AND source_max_event_assigned_at <= source_cutoff
    )
  ),
  CHECK (
    embedding IS NULL
    OR (
      provenance_status = 'verified_feature_timestamps'
      AND source_max_embedding_generated_at IS NOT NULL
      AND source_max_embedding_generated_at <= source_cutoff
    )
  ),
  CHECK (
    provenance_status <> 'historical_reconstruction_unversioned_cluster'
    OR (embedding IS NULL AND embedded_article_count = 0)
  )
);

-- Le baseline MCP matérialise d'abord le pool temporel puis calcule une distance
-- exacte. On n'ajoute donc pas encore d'index ANN : un scan HNSW filtré après coup
-- peut rendre moins de k voisins valides (ef_search) et masquer un pool pourtant sain.
CREATE INDEX IF NOT EXISTS idx_episode_representations_as_of
  ON episode_representations (as_of);

-- Défense en profondeur : un snapshot v2 est append-once. Une nouvelle méthode doit
-- passer par une migration/version explicite, jamais par un UPDATE silencieux.
CREATE OR REPLACE FUNCTION reject_episode_representation_mutation()
RETURNS trigger LANGUAGE plpgsql AS $immutable$
BEGIN
  RAISE EXCEPTION 'episode_representations is immutable (% on episode %)',
    TG_OP, OLD.episode_id;
END;
$immutable$;

DROP TRIGGER IF EXISTS episode_representations_immutable
  ON episode_representations;
CREATE TRIGGER episode_representations_immutable
BEFORE UPDATE OR DELETE ON episode_representations
FOR EACH ROW EXECUTE FUNCTION reject_episode_representation_mutation();

-- Audit explicite : état et outcome sont calculés au moment où la représentation
-- était disponible, qui peut être postérieur à l'onset narratif `t0`.
ALTER TABLE episode_market_state
  ADD COLUMN IF NOT EXISTS decision_at timestamptz;
ALTER TABLE episode_outcomes
  ADD COLUMN IF NOT EXISTS decision_at timestamptz;

-- Le serveur utilise un rôle SELECT-only. Le bloc reste idempotent sur une base de
-- développement où ce rôle n'existe pas encore.
DO $grant$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'mcp_ro') THEN
    GRANT SELECT ON TABLE episode_representations, legacy_event_taints TO mcp_ro;
  END IF;
END;
$grant$;
