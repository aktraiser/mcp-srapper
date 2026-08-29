-- 002 — Anti-fuite temporelle : à quel instant l'outcome à 3 jours est RÉELLEMENT connu.
-- find_analogs doit exiger outcome_available_at < t0(query), sinon un analogue dont
-- la fenêtre de 3 jours se termine APRÈS la requête fuiterait le futur (faux alpha).
-- = close du 3e jour de bourse après l'entrée (approx. clôture US ~21:00 UTC). Additif.
ALTER TABLE episode_outcomes ADD COLUMN IF NOT EXISTS outcome_available_at timestamptz;
CREATE INDEX IF NOT EXISTS idx_episode_outcomes_avail ON episode_outcomes (outcome_available_at);
