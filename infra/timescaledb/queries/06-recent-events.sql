SELECT event_id,
       producer_id,
       symbol,
       price,
       quantity,
       event_time,
       producer_time,
       processing_time,
       ingestion_latency_ms,
       event_lag_ms,
       scenario,
       source
FROM financial_events
ORDER BY created_at DESC
LIMIT 20;
