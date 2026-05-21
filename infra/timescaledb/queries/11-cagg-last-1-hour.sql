\timing on
SELECT bucket,
       symbol,
       event_count,
       avg_price,
       min_price,
       max_price,
       avg_latency_ms,
       max_latency_ms
FROM cagg_events_1min
WHERE bucket >= now() - interval '1 hour'
ORDER BY bucket DESC, symbol;
