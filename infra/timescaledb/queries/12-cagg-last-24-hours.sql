\timing on
SELECT bucket,
       scenario,
       event_count,
       avg_latency_ms,
       max_latency_ms
FROM cagg_events_15min
WHERE bucket >= now() - interval '24 hours'
ORDER BY bucket DESC, scenario;
