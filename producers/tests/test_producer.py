from __future__ import annotations

import json
import os
import random
import unittest
from unittest.mock import patch

from kafka.errors import KafkaError

from producers.src.producer import (
    FinancialEvent,
    MarketEventGenerator,
    ProducerConfig,
    PublicationDeliveryError,
    PublicationTracker,
    ShutdownFlag,
    encode_event,
    load_config,
    publish_loop,
)


class ImmediateFuture:
    def __init__(self, error: BaseException | None = None) -> None:
        self.error = error

    def add_callback(self, callback, *args):
        if self.error is None:
            callback(object(), *args)
        return self

    def add_errback(self, callback, *args):
        if self.error is not None:
            callback(self.error, *args)
        return self


class FakeProducer:
    def __init__(self, future: ImmediateFuture) -> None:
        self.future = future
        self.flush_calls = 0

    def send(self, *_args, **_kwargs):
        return self.future

    def flush(self, timeout: int) -> None:
        self.flush_calls += 1


def config(
    producer_type: str = "random", burst_probability: float = 0.05
) -> ProducerConfig:
    return ProducerConfig(
        producer_id="test-producer",
        producer_type=producer_type,
        bootstrap_servers="kafka:9092",
        topic="financial-events-raw",
        symbols=("PETR4",),
        rate_per_second=1_000,
        scenario="test",
        run_duration_seconds=1,
        random_seed=123,
        source="synthetic",
        volatility=0.015,
        trend_strength=0.002,
        burst_probability=burst_probability,
    )


class ProducerConfigTest(unittest.TestCase):
    def test_loads_and_normalizes_environment_configuration(self) -> None:
        """@spec:AC-011 configuracao valida e normalizada pelo ambiente."""
        environment = {
            "PRODUCER_ID": "producer-under-test",
            "PRODUCER_TYPE": " TREND ",
            "KAFKA_BOOTSTRAP_SERVERS": "broker:19092",
            "KAFKA_TOPIC_EVENTS": "legacy-raw-topic",
            "PRODUCER_SYMBOLS": " petr4, vale3 ",
            "PRODUCER_RATE_PER_SECOND": "125.5",
            "PRODUCER_SCENARIO": "medium",
            "PRODUCER_RUN_DURATION_SECONDS": "60",
            "PRODUCER_RANDOM_SEED": "2026",
            "PRODUCER_SOURCE": "test-suite",
            "PRODUCER_VOLATILITY": "0.02",
            "PRODUCER_TREND_STRENGTH": "0.003",
            "PRODUCER_BURST_PROBABILITY": "0.25",
        }

        with patch.dict(os.environ, environment, clear=True):
            loaded = load_config()

        self.assertEqual("producer-under-test", loaded.producer_id)
        self.assertEqual("trend", loaded.producer_type)
        self.assertEqual("broker:19092", loaded.bootstrap_servers)
        self.assertEqual("legacy-raw-topic", loaded.topic)
        self.assertEqual(("PETR4", "VALE3"), loaded.symbols)
        self.assertEqual(125.5, loaded.rate_per_second)
        self.assertEqual(60.0, loaded.run_duration_seconds)
        self.assertEqual(2026, loaded.random_seed)
        self.assertEqual(0.25, loaded.burst_probability)

    def test_rejects_invalid_environment_configuration(self) -> None:
        """@spec:AC-011 configuracao invalida falha de forma explicita."""
        invalid_cases = (
            ("taxa nula", {"PRODUCER_RATE_PER_SECOND": "0"}),
            ("tipo desconhecido", {"PRODUCER_TYPE": "replay"}),
            ("probabilidade maior que um", {"PRODUCER_BURST_PROBABILITY": "1.1"}),
            ("seed nao inteira", {"PRODUCER_RANDOM_SEED": "not-an-integer"}),
            ("volatilidade negativa", {"PRODUCER_VOLATILITY": "-0.01"}),
        )

        for reason, environment in invalid_cases:
            with self.subTest(reason=reason), patch.dict(os.environ, environment, clear=True):
                with self.assertRaises(ValueError):
                    load_config()


class EventEncodingTest(unittest.TestCase):
    def test_encodes_complete_event_as_compact_utf8_json(self) -> None:
        """@spec:AC-011 serializacao preserva o contrato do evento financeiro."""
        event = FinancialEvent(
            event_id="00000000-0000-0000-0000-000000000011",
            producer_id="producer-a",
            symbol="PETR4",
            price=38.75,
            quantity=250,
            event_time="2026-05-01T10:00:00.000Z",
            producer_time="2026-05-01T10:00:00.001Z",
            source="sintetico",
            scenario="low",
            sequence=11,
        )

        encoded = encode_event(event)

        self.assertIsInstance(encoded, bytes)
        self.assertNotIn(b" ", encoded)
        self.assertEqual(
            {
                "event_id": "00000000-0000-0000-0000-000000000011",
                "producer_id": "producer-a",
                "symbol": "PETR4",
                "price": 38.75,
                "quantity": 250,
                "event_time": "2026-05-01T10:00:00.000Z",
                "producer_time": "2026-05-01T10:00:00.001Z",
                "source": "sintetico",
                "scenario": "low",
                "sequence": 11,
            },
            json.loads(encoded),
        )


class MarketEventGeneratorTest(unittest.TestCase):
    def test_random_trend_and_burst_are_reproducible_with_seed(self) -> None:
        """@spec:AC-011 geradores random, trend e burst respeitam seed deterministica."""
        for producer_type in ("random", "trend", "burst"):
            with self.subTest(producer_type=producer_type):
                burst_probability = 1.0 if producer_type == "burst" else 0.05
                producer_config = config(producer_type, burst_probability)
                first = MarketEventGenerator(producer_config, random.Random(2026))
                second = MarketEventGenerator(producer_config, random.Random(2026))

                first_prices = [first.next_price("PETR4") for _ in range(3)]
                first_quantities = [first.next_quantity() for _ in range(3)]
                second_prices = [second.next_price("PETR4") for _ in range(3)]
                second_quantities = [second.next_quantity() for _ in range(3)]

                self.assertEqual(first_prices, second_prices)
                self.assertEqual(first_quantities, second_quantities)
                self.assertTrue(all(price > 0 for price in first_prices))

                if producer_type == "burst":
                    self.assertTrue(all(1_000 <= quantity <= 10_000 for quantity in first_quantities))
                elif producer_type == "trend":
                    self.assertTrue(all(100 <= quantity <= 2_000 for quantity in first_quantities))
                else:
                    self.assertTrue(all(1 <= quantity <= 1_000 for quantity in first_quantities))

    @patch("producers.src.producer.utc_now_iso", return_value="2026-05-01T10:00:00.000Z")
    @patch("producers.src.producer.uuid.uuid4", return_value="fixed-event-id")
    def test_builds_event_with_generator_configuration(self, _uuid, _utc_now) -> None:
        """@spec:AC-011 gerador produz evento completo com sequencia e metadados."""
        generator = MarketEventGenerator(config(), random.Random(7))

        event = generator.next_event(42)

        self.assertEqual("fixed-event-id", event.event_id)
        self.assertEqual("test-producer", event.producer_id)
        self.assertEqual("PETR4", event.symbol)
        self.assertGreater(event.price, 0)
        self.assertGreater(event.quantity, 0)
        self.assertEqual("2026-05-01T10:00:00.000Z", event.event_time)
        self.assertEqual("2026-05-01T10:00:00.000Z", event.producer_time)
        self.assertEqual("synthetic", event.source)
        self.assertEqual("test", event.scenario)
        self.assertEqual(42, event.sequence)


class PublicationTrackerTest(unittest.TestCase):
    def test_counts_broker_confirmations_and_failures_separately(self) -> None:
        """@spec:AC-013 callbacks distinguem confirmacoes e falhas."""
        tracker = PublicationTracker()
        success = ImmediateFuture()
        failure = ImmediateFuture(KafkaError("delivery failed"))

        tracker.record_attempt()
        tracker.watch(success, "event-1", 1)
        tracker.record_attempt()
        tracker.watch(failure, "event-2", 2)

        self.assertEqual(2, tracker.snapshot().attempted)
        self.assertEqual(1, tracker.snapshot().confirmed)
        self.assertEqual(1, tracker.snapshot().failed)


class PublishLoopTest(unittest.TestCase):
    @patch("producers.src.producer.should_continue", side_effect=[True, False])
    def test_returns_only_broker_confirmed_publications(self, _continue) -> None:
        """@spec:AC-013 publicacao confirmada produz estatisticas consistentes."""
        producer = FakeProducer(ImmediateFuture())

        stats = publish_loop(config(), producer, ShutdownFlag())

        self.assertEqual(1, stats.attempted)
        self.assertEqual(1, stats.confirmed)
        self.assertEqual(0, stats.failed)
        self.assertEqual(1, producer.flush_calls)

    @patch("producers.src.producer.should_continue", side_effect=[True, False])
    def test_raises_when_broker_reports_delivery_failure(self, _continue) -> None:
        """@spec:AC-013 falha assincrona faz a execucao retornar erro."""
        producer = FakeProducer(ImmediateFuture(KafkaError("delivery failed")))

        with self.assertRaises(PublicationDeliveryError) as raised:
            publish_loop(config(), producer, ShutdownFlag())

        self.assertEqual(1, raised.exception.stats.attempted)
        self.assertEqual(0, raised.exception.stats.confirmed)
        self.assertEqual(1, raised.exception.stats.failed)
        self.assertEqual(1, producer.flush_calls)


if __name__ == "__main__":
    unittest.main()
