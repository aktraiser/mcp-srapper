-- 001 — Market Memory : épisodes à t0 propre (sans leakage), état de marché PIT, outcomes.
-- Reproduit le schéma créé en live sur scraping_station. Idempotent (IF NOT EXISTS).
-- gen_random_uuid() = coeur PostgreSQL >= 13 (pas d'extension requise).

-- Un MarketEpisode = un événement à un instant t0 = onset du burst d'articles.
-- kind : 'point' (burst net) | 'saga_primary' (multi-burst à splitter) | 'recurring' (bruit).
CREATE TABLE IF NOT EXISTS market_episodes (
  id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  source_event_id  uuid UNIQUE,                 -- provenance : events.id
  t0               timestamptz NOT NULL,        -- instant info publique = onset du burst
  kind             text NOT NULL,
  has_multi_burst  boolean DEFAULT false,       -- saga à re-segmenter (phase 2)
  n_articles       int,
  n_domains        int,
  span_h           numeric,
  burst_peak       int,
  burstiness       numeric,
  event_type       text,
  signature_type   text,
  main_theme       text,
  main_entities    text[],
  signature        text,
  builder_version  text DEFAULT 'v1',
  created_at       timestamptz DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_market_episodes_t0   ON market_episodes (t0);
CREATE INDEX IF NOT EXISTS idx_market_episodes_kind ON market_episodes (kind);
CREATE INDEX IF NOT EXISTS idx_market_episodes_type ON market_episodes (event_type);

-- État de marché point-in-time STRICTEMENT avant t0 (sensor macro/rates/commodities/crypto).
CREATE TABLE IF NOT EXISTS episode_market_state (
  episode_id   uuid PRIMARY KEY REFERENCES market_episodes(id),
  t0           timestamptz,
  coverage_ok  boolean,                         -- t0 dans la fenêtre de données marché
  n_series     int,
  state        jsonb,                           -- {SP500,VIXCLS,DCOILWTICO,...,BTC,regime}
  built_at     timestamptz DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_episode_market_state_cov ON episode_market_state (coverage_ok);

-- Outcomes leakage-safe (entrée = 1er close >= t0). Daily = MVP ; per-stock/intraday = Alpaca.
CREATE TABLE IF NOT EXISTS episode_outcomes (
  episode_id  uuid PRIMARY KEY REFERENCES market_episodes(id),
  t0          timestamptz,
  entry_date  date,
  spx_ret_1d  numeric,
  spx_ret_3d  numeric,
  spx_ret_7d  numeric,
  vix_chg_1d  numeric,
  oil_chg_1d  numeric,
  dir_3d      int,                              -- 1 si spx_ret_3d > 0
  built_at    timestamptz DEFAULT now()
);
