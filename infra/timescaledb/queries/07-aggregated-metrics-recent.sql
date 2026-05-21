SELECT bucket_start,
       bucket_end,
       symbol,
       scenario,
       event_count,
       avg_price,
       total_quantity,
       avg_ingestion_latency_ms,
       p50_ingestion_latency_ms,
       p95_ingestion_latency_ms,
       max_ingestion_latency_ms
FROM financial_event_metrics
ORDER BY bucket_start DESC, symbol, scenario
LIMIT 50;
