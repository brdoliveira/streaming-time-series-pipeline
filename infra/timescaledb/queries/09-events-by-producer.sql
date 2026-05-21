SELECT producer_id,
       scenario,
       count(*) AS total_events,
       avg(ingestion_latency_ms) AS avg_latency_ms,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY ingestion_latency_ms) AS p95_latency_ms,
       min(event_time) AS first_event_time,
       max(event_time) AS last_event_time
FROM financial_events
GROUP BY producer_id, scenario
ORDER BY scenario, producer_id;
