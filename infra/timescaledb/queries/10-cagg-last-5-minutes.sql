\timing on
SELECT bucket,
       symbol,
       event_count,
       avg_price,
       total_quantity,
       avg_latency_ms,
       max_latency_ms
FROM cagg_events_1min
WHERE bucket >= now() - interval '5 minutes'
ORDER BY bucket DESC, symbol;
