\timing on
SELECT time_bucket('1 minute', event_time) AS bucket,
       symbol,
       avg(price) AS avg_price,
       min(price) AS min_price,
       max(price) AS max_price,
       avg(ingestion_latency_ms) AS avg_latency_ms,
       count(*) AS events
FROM financial_events
WHERE event_time >= now() - interval '1 hour'
GROUP BY bucket, symbol
ORDER BY bucket DESC, symbol;
