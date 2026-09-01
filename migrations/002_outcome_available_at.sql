-- 002 — Anti-fuite temporelle : à quel instant l'outcome à 3 jours est RÉELLEMENT connu.
-- find_analogs doit exiger outcome_available_at < decision_at(query), sinon un analogue dont
-- la fenêtre de 3 jours se termine APRÈS la requête fuiterait le futur (faux alpha).
-- Depuis le builder v2 : close UTC exact de la 3e session XNYS (DST/early closes inclus).
ALTER TABLE episode_outcomes ADD COLUMN IF NOT EXISTS outcome_available_at timestamptz;
CREATE INDEX IF NOT EXISTS idx_episode_outcomes_avail ON episode_outcomes (outcome_available_at);
