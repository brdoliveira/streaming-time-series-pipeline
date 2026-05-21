SELECT extname AS extension_name
FROM pg_extension
WHERE extname = 'timescaledb';

SELECT table_schema,
       table_name
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_name = 'financial_events';

SELECT hypertable_schema,
       hypertable_name,
       num_dimensions
FROM timescaledb_information.hypertables
WHERE hypertable_schema = 'public'
  AND hypertable_name IN ('financial_events', 'financial_event_metrics')
ORDER BY hypertable_name;

SELECT indexname
FROM pg_indexes
WHERE schemaname = 'public'
  AND tablename IN ('financial_events', 'financial_event_metrics')
ORDER BY indexname;
