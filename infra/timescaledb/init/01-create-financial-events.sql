CREATE TABLE IF NOT EXISTS financial_events (
  event_id UUID NOT NULL,
  producer_id TEXT NOT NULL DEFAULT 'unknown' CHECK (length(trim(producer_id)) > 0),
  symbol TEXT NOT NULL CHECK (symbol ~ '^[A-Za-z0-9._-]+$'),
  price NUMERIC NOT NULL CHECK (price > 0),
  quantity INTEGER NOT NULL CHECK (quantity > 0),
  event_time TIMESTAMPTZ NOT NULL,
  producer_time TIMESTAMPTZ NOT NULL,
  processing_time TIMESTAMPTZ NOT NULL,
  source TEXT NOT NULL CHECK (length(trim(source)) > 0),
  scenario TEXT NOT NULL CHECK (length(trim(scenario)) > 0),
  sequence BIGINT NOT NULL CHECK (sequence >= 0),
  ingestion_latency_ms BIGINT NOT NULL,
  event_lag_ms BIGINT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

SELECT create_hypertable('financial_events', 'event_time', if_not_exists => TRUE);

CREATE UNIQUE INDEX IF NOT EXISTS idx_financial_events_event_id_time
  ON financial_events (event_id, event_time);

CREATE INDEX IF NOT EXISTS idx_financial_events_symbol_time
  ON financial_events (symbol, event_time DESC);

CREATE INDEX IF NOT EXISTS idx_financial_events_producer_time
  ON financial_events (producer_id, event_time DESC);

CREATE INDEX IF NOT EXISTS idx_financial_events_scenario_time
  ON financial_events (scenario, event_time DESC);

CREATE INDEX IF NOT EXISTS idx_financial_events_processing_time
  ON financial_events (processing_time DESC);

CREATE INDEX IF NOT EXISTS idx_financial_events_created_at
  ON financial_events (created_at DESC);

CREATE TABLE IF NOT EXISTS financial_event_metrics (
  bucket_start TIMESTAMPTZ NOT NULL,
  bucket_end TIMESTAMPTZ NOT NULL,
  symbol TEXT NOT NULL CHECK (symbol ~ '^[A-Za-z0-9._-]+$'),
  scenario TEXT NOT NULL CHECK (length(trim(scenario)) > 0),
  event_count BIGINT NOT NULL CHECK (event_count >= 0),
  avg_price NUMERIC NOT NULL,
  min_price NUMERIC NOT NULL,
  max_price NUMERIC NOT NULL,
  total_quantity BIGINT NOT NULL CHECK (total_quantity >= 0),
  avg_ingestion_latency_ms NUMERIC NOT NULL,
  p50_ingestion_latency_ms BIGINT NOT NULL,
  p95_ingestion_latency_ms BIGINT NOT NULL,
  max_ingestion_latency_ms BIGINT NOT NULL,
  avg_event_lag_ms NUMERIC NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

SELECT create_hypertable('financial_event_metrics', 'bucket_start', if_not_exists => TRUE);

CREATE UNIQUE INDEX IF NOT EXISTS idx_financial_event_metrics_bucket_symbol_scenario
  ON financial_event_metrics (bucket_start, symbol, scenario);

CREATE INDEX IF NOT EXISTS idx_financial_event_metrics_symbol_bucket
  ON financial_event_metrics (symbol, bucket_start DESC);

CREATE INDEX IF NOT EXISTS idx_financial_event_metrics_scenario_bucket
  ON financial_event_metrics (scenario, bucket_start DESC);
