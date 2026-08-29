-- Queries semanticamente equivalentes para o benchmark raw versus CAGG.
-- O runner substitui __WINDOW_START__ e __WINDOW_END__ por limites UTC
-- alinhados em 15 minutos e executa cada bloco separadamente.

-- BEGIN RAW_QUERY
SELECT time_bucket('15 minutes', event_time)                    AS bucket,
       symbol,
       scenario,
       count(*)                                                 AS event_count,
       round(avg(price)::numeric, 6)                            AS avg_price,
       min(price)                                               AS min_price,
       max(price)                                               AS max_price,
       sum(quantity)                                            AS total_quantity,
       round(avg(ingestion_latency_ms)::numeric, 3)             AS avg_latency_ms,
       max(ingestion_latency_ms)                                AS max_latency_ms
FROM financial_events
WHERE event_time >= TIMESTAMPTZ '__WINDOW_START__'
  AND event_time <  TIMESTAMPTZ '__WINDOW_END__'
GROUP BY time_bucket('15 minutes', event_time), symbol, scenario
ORDER BY bucket, symbol, scenario;
-- END RAW_QUERY

-- BEGIN CAGG_QUERY
SELECT bucket,
       symbol,
       scenario,
       event_count,
       round(avg_price::numeric, 6)                             AS avg_price,
       min_price,
       max_price,
       total_quantity,
       round(avg_latency_ms::numeric, 3)                        AS avg_latency_ms,
       max_latency_ms
FROM cagg_events_15min
WHERE bucket >= TIMESTAMPTZ '__WINDOW_START__'
  AND bucket <  TIMESTAMPTZ '__WINDOW_END__'
ORDER BY bucket, symbol, scenario;
-- END CAGG_QUERY
