-- Continuous aggregate: buckets de 1 minuto sobre financial_events
CREATE MATERIALIZED VIEW cagg_events_1min
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 minute', event_time) AS bucket,
       symbol,
       scenario,
       count(*)                        AS event_count,
       avg(price)                      AS avg_price,
       min(price)                      AS min_price,
       max(price)                      AS max_price,
       sum(quantity)                   AS total_quantity,
       avg(ingestion_latency_ms)       AS avg_latency_ms,
       max(ingestion_latency_ms)       AS max_latency_ms,
       stddev(ingestion_latency_ms)    AS stddev_latency_ms
FROM financial_events
GROUP BY bucket, symbol, scenario
WITH NO DATA;

SELECT add_continuous_aggregate_policy('cagg_events_1min',
  start_offset      => INTERVAL '2 hours',
  end_offset        => INTERVAL '1 minute',
  schedule_interval => INTERVAL '30 seconds');

-- Hierarchical continuous aggregate: buckets de 15 minutos sobre cagg_events_1min
CREATE MATERIALIZED VIEW cagg_events_15min
WITH (timescaledb.continuous) AS
SELECT time_bucket('15 minutes', bucket) AS bucket,
       symbol,
       scenario,
       sum(event_count)        AS event_count,
       avg(avg_price)          AS avg_price,
       min(min_price)          AS min_price,
       max(max_price)          AS max_price,
       sum(total_quantity)     AS total_quantity,
       avg(avg_latency_ms)     AS avg_latency_ms,
       max(max_latency_ms)     AS max_latency_ms
FROM cagg_events_1min
GROUP BY time_bucket('15 minutes', bucket), symbol, scenario
WITH NO DATA;

SELECT add_continuous_aggregate_policy('cagg_events_15min',
  start_offset      => INTERVAL '26 hours',
  end_offset        => INTERVAL '15 minutes',
  schedule_interval => INTERVAL '2 minutes');
