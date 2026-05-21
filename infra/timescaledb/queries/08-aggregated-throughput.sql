SELECT bucket_start AS time,
       scenario,
       sum(event_count) AS events,
       extract(epoch FROM max(bucket_end) - min(bucket_start)) AS window_seconds,
       sum(event_count) / nullif(extract(epoch FROM max(bucket_end) - min(bucket_start)), 0) AS events_per_second
FROM financial_event_metrics
GROUP BY bucket_start, scenario
ORDER BY bucket_start DESC, scenario
LIMIT 120;
