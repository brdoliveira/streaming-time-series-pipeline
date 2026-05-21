package br.com.pipeline;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FinancialEventsJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(FinancialEventsJob.class);
    private static final Pattern SYMBOL_PATTERN = Pattern.compile("^[A-Za-z0-9._-]+$");
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .setSerializationInclusion(JsonInclude.Include.NON_NULL);
    private static final OutputTag<String> INVALID_EVENTS = new OutputTag<>("invalid-events") {};

    public static void main(String[] args) throws Exception {
        JobConfig config = JobConfig.fromEnv();
        LOGGER.info("Starting FinancialEventsJob with config={}", config.safeSummary());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(config.flinkParallelism);
        env.enableCheckpointing(config.checkpointIntervalMs, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(120_000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5_000);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(config.kafkaBootstrapServers)
                .setTopics(config.kafkaTopicRaw)
                .setGroupId(config.flinkKafkaGroupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> rawEvents = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "financial-events-kafka-source");

        SingleOutputStreamOperator<ProcessedFinancialEvent> validEvents = rawEvents
                .process(new ValidateAndEnrichEventFunction())
                .name("validate-and-enrich-financial-events");

        KafkaSink<String> processedEventsSink = KafkaSink.<String>builder()
                .setBootstrapServers(config.kafkaBootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(config.kafkaTopicProcessed)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        validEvents
                .map(FinancialEventsJob::processedEventJson)
                .name("serialize-processed-events")
                .sinkTo(processedEventsSink)
                .name("processed-events-kafka-sink");

        validEvents
                .addSink(JdbcSink.sink(
                        insertSql(),
                        FinancialEventsJob::bindStatement,
                        JdbcExecutionOptions.builder()
                                .withBatchSize(config.jdbcBatchSize)
                                .withBatchIntervalMs(config.jdbcBatchIntervalMs)
                                .withMaxRetries(config.jdbcMaxRetries)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl(config.jdbcUrl())
                                .withDriverName("org.postgresql.Driver")
                                .withUsername(config.postgresUser)
                                .withPassword(config.postgresPassword)
                                .build()))
                .name("timescaledb-financial-events-sink");

        validEvents
                .keyBy(ProcessedFinancialEvent::metricsKey)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(config.metricsWindowSeconds)))
                .process(new MetricsWindowFunction())
                .name("aggregate-financial-event-metrics")
                .addSink(JdbcSink.sink(
                        metricsInsertSql(),
                        FinancialEventsJob::bindMetricStatement,
                        JdbcExecutionOptions.builder()
                                .withBatchSize(config.jdbcBatchSize)
                                .withBatchIntervalMs(config.jdbcBatchIntervalMs)
                                .withMaxRetries(config.jdbcMaxRetries)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl(config.jdbcUrl())
                                .withDriverName("org.postgresql.Driver")
                                .withUsername(config.postgresUser)
                                .withPassword(config.postgresPassword)
                                .build()))
                .name("timescaledb-financial-event-metrics-sink");

        KafkaSink<String> invalidEventsSink = KafkaSink.<String>builder()
                .setBootstrapServers(config.kafkaBootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(config.kafkaTopicErrors)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        validEvents
                .getSideOutput(INVALID_EVENTS)
                .sinkTo(invalidEventsSink)
                .name("invalid-events-kafka-sink");

        env.execute("financial-events-processing");
    }

    private static String insertSql() {
        return """
                INSERT INTO financial_events (
                  event_id,
                  producer_id,
                  symbol,
                  price,
                  quantity,
                  event_time,
                  producer_time,
                  processing_time,
                  source,
                  scenario,
                  sequence,
                  ingestion_latency_ms,
                  event_lag_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (event_id, event_time) DO NOTHING
                """;
    }

    private static String metricsInsertSql() {
        return """
                INSERT INTO financial_event_metrics (
                  bucket_start,
                  bucket_end,
                  symbol,
                  scenario,
                  event_count,
                  avg_price,
                  min_price,
                  max_price,
                  total_quantity,
                  avg_ingestion_latency_ms,
                  p50_ingestion_latency_ms,
                  p95_ingestion_latency_ms,
                  max_ingestion_latency_ms,
                  avg_event_lag_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (bucket_start, symbol, scenario)
                DO UPDATE SET
                  bucket_end = EXCLUDED.bucket_end,
                  event_count = EXCLUDED.event_count,
                  avg_price = EXCLUDED.avg_price,
                  min_price = EXCLUDED.min_price,
                  max_price = EXCLUDED.max_price,
                  total_quantity = EXCLUDED.total_quantity,
                  avg_ingestion_latency_ms = EXCLUDED.avg_ingestion_latency_ms,
                  p50_ingestion_latency_ms = EXCLUDED.p50_ingestion_latency_ms,
                  p95_ingestion_latency_ms = EXCLUDED.p95_ingestion_latency_ms,
                  max_ingestion_latency_ms = EXCLUDED.max_ingestion_latency_ms,
                  avg_event_lag_ms = EXCLUDED.avg_event_lag_ms,
                  created_at = now()
                """;
    }

    private static void bindStatement(PreparedStatement statement, ProcessedFinancialEvent event) throws java.sql.SQLException {
        statement.setObject(1, UUID.fromString(event.eventId));
        statement.setString(2, event.producerId);
        statement.setString(3, event.symbol);
        statement.setBigDecimal(4, event.price);
        statement.setInt(5, event.quantity);
        statement.setTimestamp(6, Timestamp.from(event.eventTime));
        statement.setTimestamp(7, Timestamp.from(event.producerTime));
        statement.setTimestamp(8, Timestamp.from(event.processingTime));
        statement.setString(9, event.source);
        statement.setString(10, event.scenario);
        statement.setLong(11, event.sequence);
        statement.setLong(12, event.ingestionLatencyMs);
        statement.setLong(13, event.eventLagMs);
    }

    private static void bindMetricStatement(PreparedStatement statement, FinancialEventMetric metric) throws java.sql.SQLException {
        statement.setTimestamp(1, Timestamp.from(metric.bucketStart));
        statement.setTimestamp(2, Timestamp.from(metric.bucketEnd));
        statement.setString(3, metric.symbol);
        statement.setString(4, metric.scenario);
        statement.setLong(5, metric.eventCount);
        statement.setBigDecimal(6, metric.avgPrice);
        statement.setBigDecimal(7, metric.minPrice);
        statement.setBigDecimal(8, metric.maxPrice);
        statement.setLong(9, metric.totalQuantity);
        statement.setBigDecimal(10, metric.avgIngestionLatencyMs);
        statement.setLong(11, metric.p50IngestionLatencyMs);
        statement.setLong(12, metric.p95IngestionLatencyMs);
        statement.setLong(13, metric.maxIngestionLatencyMs);
        statement.setBigDecimal(14, metric.avgEventLagMs);
    }

    private static String processedEventJson(ProcessedFinancialEvent event) throws JsonProcessingException {
        ProcessedEventJson json = new ProcessedEventJson();
        json.event_id = event.eventId;
        json.producer_id = event.producerId;
        json.symbol = event.symbol;
        json.price = event.price;
        json.quantity = event.quantity;
        json.event_time = event.eventTime.toString();
        json.producer_time = event.producerTime.toString();
        json.processing_time = event.processingTime.toString();
        json.source = event.source;
        json.scenario = event.scenario;
        json.sequence = event.sequence;
        json.ingestion_latency_ms = event.ingestionLatencyMs;
        json.event_lag_ms = event.eventLagMs;
        json.valid = true;
        return MAPPER.writeValueAsString(json);
    }

    public static class ValidateAndEnrichEventFunction extends ProcessFunction<String, ProcessedFinancialEvent> {
        @Override
        public void processElement(String raw, Context context, Collector<ProcessedFinancialEvent> out) {
            Instant processingTime = Instant.now();
            try {
                FinancialEvent event = MAPPER.readValue(raw, FinancialEvent.class);
                validate(event);

                ProcessedFinancialEvent processed = new ProcessedFinancialEvent();
                processed.eventId = event.event_id;
                processed.producerId = isBlank(event.producer_id) ? "unknown" : event.producer_id;
                processed.symbol = event.symbol.toUpperCase(Locale.ROOT);
                processed.price = event.price;
                processed.quantity = event.quantity;
                processed.eventTime = Instant.parse(event.event_time);
                processed.producerTime = Instant.parse(event.producer_time);
                processed.processingTime = processingTime;
                processed.source = event.source;
                processed.scenario = event.scenario;
                processed.sequence = event.sequence;
                processed.ingestionLatencyMs = Math.max(0, Duration.between(processed.producerTime, processingTime).toMillis());
                processed.eventLagMs = Math.max(0, Duration.between(processed.eventTime, processingTime).toMillis());
                out.collect(processed);
            } catch (Exception ex) {
                LOGGER.warn("Invalid event rejected: {}", ex.getMessage());
                context.output(INVALID_EVENTS, invalidEventJson(raw, processingTime, ex.getMessage()));
            }
        }

        private static void validate(FinancialEvent event) {
            if (isBlank(event.event_id)) {
                throw new IllegalArgumentException("event_id is required");
            }
            try {
                UUID.fromString(event.event_id);
            } catch (IllegalArgumentException ex) {
                throw new IllegalArgumentException("event_id must be a UUID");
            }
            if (isBlank(event.symbol) || !SYMBOL_PATTERN.matcher(event.symbol).matches()) {
                throw new IllegalArgumentException("symbol is required and must match [A-Za-z0-9._-]+");
            }
            if (event.price == null || event.price.compareTo(BigDecimal.ZERO) <= 0) {
                throw new IllegalArgumentException("price must be greater than zero");
            }
            if (event.quantity == null || event.quantity <= 0) {
                throw new IllegalArgumentException("quantity must be greater than zero");
            }
            if (isBlank(event.event_time)) {
                throw new IllegalArgumentException("event_time is required");
            }
            if (isBlank(event.producer_time)) {
                throw new IllegalArgumentException("producer_time is required");
            }
            Instant.parse(event.event_time);
            Instant.parse(event.producer_time);
            if (isBlank(event.source)) {
                throw new IllegalArgumentException("source is required");
            }
            if (isBlank(event.scenario)) {
                throw new IllegalArgumentException("scenario is required");
            }
            if (event.sequence == null || event.sequence < 0) {
                throw new IllegalArgumentException("sequence must be greater than or equal to zero");
            }
        }

        private static boolean isBlank(String value) {
            return value == null || value.trim().isEmpty();
        }

        private static String invalidEventJson(String raw, Instant processingTime, String error) {
            JsonNode rawJson = null;
            try {
                rawJson = MAPPER.readTree(raw);
            } catch (JsonProcessingException ignored) {
                // Keep malformed JSON as raw text.
            }

            InvalidEvent invalid = new InvalidEvent();
            invalid.raw = rawJson == null ? raw : null;
            invalid.payload = rawJson;
            invalid.valid = false;
            invalid.validation_error = error;
            invalid.processing_time = processingTime.toString();
            try {
                return MAPPER.writeValueAsString(invalid);
            } catch (JsonProcessingException ex) {
                return "{\"valid\":false,\"validation_error\":\"failed to serialize invalid event\"}";
            }
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class FinancialEvent {
        public String event_id;
        public String producer_id;
        public String symbol;
        public BigDecimal price;
        public Integer quantity;
        public String event_time;
        public String producer_time;
        public String source;
        public String scenario;
        public Long sequence;
    }

    public static class ProcessedFinancialEvent {
        public String eventId;
        public String producerId;
        public String symbol;
        public BigDecimal price;
        public int quantity;
        public Instant eventTime;
        public Instant producerTime;
        public Instant processingTime;
        public String source;
        public String scenario;
        public long sequence;
        public long ingestionLatencyMs;
        public long eventLagMs;

        public String metricsKey() {
            return symbol + "|" + scenario;
        }
    }

    public static class MetricsWindowFunction
            extends ProcessWindowFunction<ProcessedFinancialEvent, FinancialEventMetric, String, TimeWindow> {
        @Override
        public void process(
                String key,
                Context context,
                Iterable<ProcessedFinancialEvent> events,
                Collector<FinancialEventMetric> out) {
            List<ProcessedFinancialEvent> values = new ArrayList<>();
            events.forEach(values::add);
            if (values.isEmpty()) {
                return;
            }

            String[] keyParts = key.split("\\|", 2);
            String symbol = keyParts[0];
            String scenario = keyParts.length > 1 ? keyParts[1] : "unknown";

            BigDecimal totalPrice = BigDecimal.ZERO;
            BigDecimal minPrice = values.get(0).price;
            BigDecimal maxPrice = values.get(0).price;
            long totalQuantity = 0;
            long totalLatency = 0;
            long totalLag = 0;
            long maxLatency = Long.MIN_VALUE;
            List<Long> latencies = new ArrayList<>();

            for (ProcessedFinancialEvent event : values) {
                totalPrice = totalPrice.add(event.price);
                minPrice = minPrice.min(event.price);
                maxPrice = maxPrice.max(event.price);
                totalQuantity += event.quantity;
                totalLatency += event.ingestionLatencyMs;
                totalLag += event.eventLagMs;
                maxLatency = Math.max(maxLatency, event.ingestionLatencyMs);
                latencies.add(event.ingestionLatencyMs);
            }

            latencies.sort(Comparator.naturalOrder());
            long count = values.size();

            FinancialEventMetric metric = new FinancialEventMetric();
            metric.bucketStart = Instant.ofEpochMilli(context.window().getStart());
            metric.bucketEnd = Instant.ofEpochMilli(context.window().getEnd());
            metric.symbol = symbol;
            metric.scenario = scenario;
            metric.eventCount = count;
            metric.avgPrice = totalPrice.divide(BigDecimal.valueOf(count), 6, java.math.RoundingMode.HALF_UP);
            metric.minPrice = minPrice;
            metric.maxPrice = maxPrice;
            metric.totalQuantity = totalQuantity;
            metric.avgIngestionLatencyMs = BigDecimal.valueOf(totalLatency)
                    .divide(BigDecimal.valueOf(count), 2, java.math.RoundingMode.HALF_UP);
            metric.p50IngestionLatencyMs = percentile(latencies, 0.50);
            metric.p95IngestionLatencyMs = percentile(latencies, 0.95);
            metric.maxIngestionLatencyMs = maxLatency;
            metric.avgEventLagMs = BigDecimal.valueOf(totalLag)
                    .divide(BigDecimal.valueOf(count), 2, java.math.RoundingMode.HALF_UP);
            out.collect(metric);
        }

        private static long percentile(List<Long> sortedValues, double percentile) {
            if (sortedValues.isEmpty()) {
                return 0;
            }
            int index = (int) Math.ceil(percentile * sortedValues.size()) - 1;
            return sortedValues.get(Math.max(0, Math.min(index, sortedValues.size() - 1)));
        }
    }

    public static class FinancialEventMetric {
        public Instant bucketStart;
        public Instant bucketEnd;
        public String symbol;
        public String scenario;
        public long eventCount;
        public BigDecimal avgPrice;
        public BigDecimal minPrice;
        public BigDecimal maxPrice;
        public long totalQuantity;
        public BigDecimal avgIngestionLatencyMs;
        public long p50IngestionLatencyMs;
        public long p95IngestionLatencyMs;
        public long maxIngestionLatencyMs;
        public BigDecimal avgEventLagMs;
    }

    public static class ProcessedEventJson {
        public String event_id;
        public String producer_id;
        public String symbol;
        public BigDecimal price;
        public int quantity;
        public String event_time;
        public String producer_time;
        public String processing_time;
        public String source;
        public String scenario;
        public long sequence;
        public long ingestion_latency_ms;
        public long event_lag_ms;
        public boolean valid;
    }

    public static class InvalidEvent {
        public boolean valid;
        public String validation_error;
        public String processing_time;
        public String raw;
        public JsonNode payload;
    }

    public static class JobConfig {
        public final String kafkaBootstrapServers;
        public final String kafkaTopicRaw;
        public final String kafkaTopicEvents;
        public final String kafkaTopicProcessed;
        public final String kafkaTopicErrors;
        public final String flinkKafkaGroupId;
        public final String postgresHost;
        public final int postgresPort;
        public final String postgresDb;
        public final String postgresUser;
        public final String postgresPassword;
        public final int flinkParallelism;
        public final long checkpointIntervalMs;
        public final int jdbcBatchSize;
        public final long jdbcBatchIntervalMs;
        public final int jdbcMaxRetries;
        public final long metricsWindowSeconds;

        private JobConfig(Properties env) {
            this.kafkaBootstrapServers = env.getProperty("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
            this.kafkaTopicRaw = firstNonBlank(env, "KAFKA_TOPIC_RAW", "KAFKA_TOPIC_EVENTS", "financial-events-raw");
            this.kafkaTopicEvents = env.getProperty("KAFKA_TOPIC_EVENTS", "financial-events");
            this.kafkaTopicProcessed = env.getProperty("KAFKA_TOPIC_PROCESSED", "financial-events-processed");
            this.kafkaTopicErrors = env.getProperty("KAFKA_TOPIC_ERRORS", "financial-events-invalid");
            this.flinkKafkaGroupId = env.getProperty("FLINK_KAFKA_GROUP_ID", "financial-events-flink");
            this.postgresHost = env.getProperty("POSTGRES_HOST", "timescaledb");
            this.postgresPort = intValue(env, "POSTGRES_PORT", 5432);
            this.postgresDb = env.getProperty("POSTGRES_DB", "pipeline");
            this.postgresUser = env.getProperty("POSTGRES_USER", "pipeline");
            this.postgresPassword = env.getProperty("POSTGRES_PASSWORD", "pipeline");
            this.flinkParallelism = intValue(env, "FLINK_PARALLELISM", 3);
            this.checkpointIntervalMs = longValue(env, "FLINK_CHECKPOINT_INTERVAL_MS", 30_000);
            this.jdbcBatchSize = intValue(env, "JDBC_BATCH_SIZE", 500);
            this.jdbcBatchIntervalMs = longValue(env, "JDBC_BATCH_INTERVAL_MS", 1_000);
            this.jdbcMaxRetries = intValue(env, "JDBC_MAX_RETRIES", 3);
            this.metricsWindowSeconds = longValue(env, "METRICS_WINDOW_SECONDS", 10);
        }

        static JobConfig fromEnv() {
            Properties props = new Properties();
            System.getenv().forEach(props::setProperty);
            return new JobConfig(props);
        }

        String jdbcUrl() {
            return "jdbc:postgresql://" + postgresHost + ":" + postgresPort + "/" + postgresDb;
        }

        String safeSummary() {
            return "kafkaBootstrapServers=" + kafkaBootstrapServers
                    + ", kafkaTopicRaw=" + kafkaTopicRaw
                    + ", kafkaTopicEvents=" + kafkaTopicEvents
                    + ", kafkaTopicProcessed=" + kafkaTopicProcessed
                    + ", kafkaTopicErrors=" + kafkaTopicErrors
                    + ", flinkKafkaGroupId=" + flinkKafkaGroupId
                    + ", jdbcUrl=" + jdbcUrl()
                    + ", postgresUser=" + postgresUser
                    + ", flinkParallelism=" + flinkParallelism
                    + ", checkpointIntervalMs=" + checkpointIntervalMs
                    + ", jdbcBatchSize=" + jdbcBatchSize
                    + ", jdbcBatchIntervalMs=" + jdbcBatchIntervalMs
                    + ", jdbcMaxRetries=" + jdbcMaxRetries
                    + ", metricsWindowSeconds=" + metricsWindowSeconds;
        }

        private static String firstNonBlank(Properties env, String preferredKey, String fallbackKey, String defaultValue) {
            String preferred = env.getProperty(preferredKey);
            if (preferred != null && !preferred.isBlank()) {
                return preferred;
            }
            String fallback = env.getProperty(fallbackKey);
            if (fallback != null && !fallback.isBlank()) {
                return fallback;
            }
            return defaultValue;
        }

        private static int intValue(Properties env, String key, int defaultValue) {
            return Integer.parseInt(env.getProperty(key, String.valueOf(defaultValue)));
        }

        private static long longValue(Properties env, String key, long defaultValue) {
            return Long.parseLong(env.getProperty(key, String.valueOf(defaultValue)));
        }
    }
}
