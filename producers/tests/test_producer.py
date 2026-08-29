from __future__ import annotations

import unittest
from unittest.mock import patch

from kafka.errors import KafkaError

from producers.src.producer import (
    ProducerConfig,
    PublicationDeliveryError,
    PublicationTracker,
    ShutdownFlag,
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


def config() -> ProducerConfig:
    return ProducerConfig(
        producer_id="test-producer",
        producer_type="random",
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
        burst_probability=0.05,
    )


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
