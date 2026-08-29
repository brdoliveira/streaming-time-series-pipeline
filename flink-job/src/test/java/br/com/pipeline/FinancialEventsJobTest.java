package br.com.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import br.com.pipeline.FinancialEventsJob.FinancialEventMetric;
import br.com.pipeline.FinancialEventsJob.JobConfig;
import br.com.pipeline.FinancialEventsJob.MetricsWindowFunction;
import br.com.pipeline.FinancialEventsJob.ProcessedFinancialEvent;
import br.com.pipeline.FinancialEventsJob.ValidateAndEnrichEventFunction;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class FinancialEventsJobTest {
    private static final Instant PROCESSING_TIME = Instant.parse("2026-05-01T10:00:02Z");

    @Test
    @DisplayName("@spec:AC-010 enriquece um evento valido com campos e latencias calculadas")
    void enrichesValidEvent() throws Exception {
        ProcessedFinancialEvent event = ValidateAndEnrichEventFunction.validateAndEnrich(
                validJson("123.45", 10, "petr4", ""), PROCESSING_TIME);

        assertEquals("00000000-0000-0000-0000-000000000001", event.eventId);
        assertEquals("unknown", event.producerId);
        assertEquals("PETR4", event.symbol);
        assertEquals(new BigDecimal("123.45"), event.price);
        assertEquals(10, event.quantity);
        assertEquals(2_000, event.ingestionLatencyMs);
        assertEquals(3_000, event.eventLagMs);
        assertEquals("PETR4|test", event.metricsKey());
    }

    @ParameterizedTest(name = "@spec:AC-010 rejeita evento invalido: {0}")
    @MethodSource("invalidEvents")
    void rejectsInvalidEvent(String reason, String json) {
        assertThrows(
                IllegalArgumentException.class,
                () -> ValidateAndEnrichEventFunction.validateAndEnrich(json, PROCESSING_TIME),
                reason);
    }

    static Stream<Arguments> invalidEvents() {
        return Stream.of(
                Arguments.of("UUID invalido", validJson("123.45", 10, "PETR4", "producer-1")
                        .replace("00000000-0000-0000-0000-000000000001", "not-a-uuid")),
                Arguments.of("preco nao positivo", validJson("0", 10, "PETR4", "producer-1")),
                Arguments.of("quantidade nao positiva", validJson("123.45", 0, "PETR4", "producer-1")),
                Arguments.of("simbolo invalido", validJson("123.45", 10, "PETR 4", "producer-1")));
    }

    @Test
    @DisplayName("@spec:AC-010 carrega configuracao padrao e sobrescritas fornecidas")
    void loadsConfigurationDefaultsAndOverrides() {
        JobConfig defaults = JobConfig.fromProperties(new Properties());

        assertEquals("kafka:9092", defaults.kafkaBootstrapServers);
        assertEquals("financial-events-raw", defaults.kafkaTopicRaw);
        assertEquals(3, defaults.flinkParallelism);
        assertEquals(500, defaults.jdbcBatchSize);
        assertEquals("jdbc:postgresql://timescaledb:5432/pipeline", defaults.jdbcUrl());

        Properties overrides = new Properties();
        overrides.setProperty("KAFKA_TOPIC_EVENTS", "legacy-raw");
        overrides.setProperty("POSTGRES_HOST", "database");
        overrides.setProperty("POSTGRES_PORT", "5544");
        overrides.setProperty("POSTGRES_DB", "experiment");
        overrides.setProperty("FLINK_PARALLELISM", "6");
        overrides.setProperty("JDBC_BATCH_SIZE", "1000");
        JobConfig configured = JobConfig.fromProperties(overrides);

        assertEquals("legacy-raw", configured.kafkaTopicRaw);
        assertEquals(6, configured.flinkParallelism);
        assertEquals(1_000, configured.jdbcBatchSize);
        assertEquals("jdbc:postgresql://database:5544/experiment", configured.jdbcUrl());
    }

    @Test
    @DisplayName("@spec:AC-015 carrega caminho configuravel para checkpoints persistentes")
    void loadsCheckpointStorageConfiguration() {
        JobConfig defaults = JobConfig.fromProperties(new Properties());
        assertEquals("file:///opt/flink/checkpoints", defaults.checkpointStorage);

        Properties overrides = new Properties();
        overrides.setProperty("FLINK_CHECKPOINT_STORAGE", "file:///custom/checkpoints");
        JobConfig configured = JobConfig.fromProperties(overrides);

        assertEquals("file:///custom/checkpoints", configured.checkpointStorage);
    }

    @Test
    @DisplayName("@spec:AC-010 calcula percentis pela regra nearest-rank")
    void calculatesNearestRankPercentiles() {
        List<Long> sorted = List.of(10L, 20L, 30L, 40L, 50L);

        assertEquals(30, MetricsWindowFunction.percentile(sorted, 0.50));
        assertEquals(50, MetricsWindowFunction.percentile(sorted, 0.95));
        assertEquals(0, MetricsWindowFunction.percentile(List.of(), 0.95));
    }

    @Test
    @DisplayName("@spec:AC-010 agrega contagem, precos, quantidades, latencias e percentis")
    void aggregatesWindowMetrics() {
        Instant start = Instant.parse("2026-05-01T10:00:00Z");
        Instant end = Instant.parse("2026-05-01T10:00:10Z");
        List<ProcessedFinancialEvent> events = new ArrayList<>();
        events.add(processedEvent("10.00", 2, 10, 5));
        events.add(processedEvent("20.00", 3, 40, 15));

        FinancialEventMetric metric = MetricsWindowFunction.aggregateMetric(
                "PETR4|high", start, end, events);

        assertEquals(start, metric.bucketStart);
        assertEquals(end, metric.bucketEnd);
        assertEquals("PETR4", metric.symbol);
        assertEquals("high", metric.scenario);
        assertEquals(2, metric.eventCount);
        assertEquals(new BigDecimal("15.000000"), metric.avgPrice);
        assertEquals(new BigDecimal("10.00"), metric.minPrice);
        assertEquals(new BigDecimal("20.00"), metric.maxPrice);
        assertEquals(5, metric.totalQuantity);
        assertEquals(new BigDecimal("25.00"), metric.avgIngestionLatencyMs);
        assertEquals(10, metric.p50IngestionLatencyMs);
        assertEquals(40, metric.p95IngestionLatencyMs);
        assertEquals(40, metric.maxIngestionLatencyMs);
        assertEquals(new BigDecimal("10.00"), metric.avgEventLagMs);
    }

    private static ProcessedFinancialEvent processedEvent(
            String price, int quantity, long latency, long lag) {
        ProcessedFinancialEvent event = new ProcessedFinancialEvent();
        event.price = new BigDecimal(price);
        event.quantity = quantity;
        event.ingestionLatencyMs = latency;
        event.eventLagMs = lag;
        return event;
    }

    private static String validJson(String price, int quantity, String symbol, String producerId) {
        return """
                {
                  "event_id": "00000000-0000-0000-0000-000000000001",
                  "producer_id": "%s",
                  "symbol": "%s",
                  "price": %s,
                  "quantity": %d,
                  "event_time": "2026-05-01T09:59:59Z",
                  "producer_time": "2026-05-01T10:00:00Z",
                  "source": "synthetic",
                  "scenario": "test",
                  "sequence": 1
                }
                """.formatted(producerId, symbol, price, quantity);
    }
}
