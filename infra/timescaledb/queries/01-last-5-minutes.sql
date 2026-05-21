\timing on
SELECT time_bucket('10 seconds', event_time) AS bucket,
       symbol,
       avg(price) AS avg_price,
       sum(quantity) AS total_quantity,
       avg(ingestion_latency_ms) AS avg_latency_ms,
       count(*) AS events
FROM financial_events
WHERE event_time >= now() - interval '5 minutes'
GROUP BY bucket, symbol
ORDER BY bucket DESC, symbol;
