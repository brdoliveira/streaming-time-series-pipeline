SELECT scenario,
       count(*) AS total_events,
       avg(ingestion_latency_ms) AS avg_latency_ms,
       percentile_cont(0.50) WITHIN GROUP (ORDER BY ingestion_latency_ms) AS p50_latency_ms,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY ingestion_latency_ms) AS p95_latency_ms,
       max(ingestion_latency_ms) AS max_latency_ms,
       avg(event_lag_ms) AS avg_event_lag_ms
FROM financial_events
GROUP BY scenario
ORDER BY scenario;
