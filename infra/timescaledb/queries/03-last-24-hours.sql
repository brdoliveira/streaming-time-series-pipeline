\timing on
SELECT time_bucket('15 minutes', event_time) AS bucket,
       scenario,
       count(*) AS events,
       avg(ingestion_latency_ms) AS avg_latency_ms,
       percentile_cont(0.50) WITHIN GROUP (ORDER BY ingestion_latency_ms) AS p50_latency_ms,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY ingestion_latency_ms) AS p95_latency_ms,
       max(ingestion_latency_ms) AS max_latency_ms
FROM financial_events
WHERE event_time >= now() - interval '24 hours'
GROUP BY bucket, scenario
ORDER BY bucket DESC, scenario;
