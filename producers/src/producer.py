from __future__ import annotations

import json
import logging
import os
import random
import signal
import socket
import sys
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable


LOGGER = logging.getLogger("financial-event-producer")
ALLOWED_PRODUCER_TYPES = {"random", "trend", "burst"}

DEFAULT_SYMBOLS = (
    "PETR4",
    "VALE3",
    "ITUB4",
    "BBDC4",
    "ABEV3",
    "MGLU3",
    "BOVA11",
    "AAPL",
    "MSFT",
    "BTC-USD",
)

BASE_PRICES = {
    "PETR4": Decimal("38.00"),
    "VALE3": Decimal("62.00"),
    "ITUB4": Decimal("34.00"),
    "BBDC4": Decimal("14.00"),
    "ABEV3": Decimal("12.00"),
    "MGLU3": Decimal("2.00"),
    "BOVA11": Decimal("125.00"),
    "AAPL": Decimal("170.00"),
    "MSFT": Decimal("420.00"),
    "BTC-USD": Decimal("65000.00"),
}


@dataclass(frozen=True)
class FinancialEvent:
    event_id: str
    producer_id: str
    symbol: str
    price: float
    quantity: int
    event_time: str
    producer_time: str
    source: str
    scenario: str
    sequence: int


@dataclass(frozen=True)
class ProducerConfig:
    producer_id: str
    producer_type: str
    bootstrap_servers: str
    topic: str
    symbols: tuple[str, ...]
    rate_per_second: float
    scenario: str
    run_duration_seconds: float
    random_seed: int | None
    source: str
    volatility: float
    trend_strength: float
    burst_probability: float


class ShutdownFlag:
    def __init__(self) -> None:
        self.requested = False

    def stop(self, *_args: object) -> None:
        self.requested = True
        LOGGER.info("shutdown requested; flushing producer")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_symbols(raw: str | None) -> tuple[str, ...]:
    if not raw:
        return DEFAULT_SYMBOLS
    symbols = tuple(symbol.strip().upper() for symbol in raw.split(",") if symbol.strip())
    return symbols or DEFAULT_SYMBOLS


def parse_float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be numeric, got {raw!r}") from exc
    if value < 0:
        raise ValueError(f"{name} must be greater than or equal to zero")
    return value


def parse_int_env(name: str) -> int | None:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return None
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc


def load_config() -> ProducerConfig:
    rate = parse_float_env("PRODUCER_RATE_PER_SECOND", 10.0)
    if rate <= 0:
        raise ValueError("PRODUCER_RATE_PER_SECOND must be greater than zero")

    producer_type = os.getenv("PRODUCER_TYPE", "random").strip().lower()
    if producer_type not in ALLOWED_PRODUCER_TYPES:
        allowed = ", ".join(sorted(ALLOWED_PRODUCER_TYPES))
        raise ValueError(f"PRODUCER_TYPE must be one of: {allowed}")

    burst_probability = parse_float_env("PRODUCER_BURST_PROBABILITY", 0.05)
    if burst_probability > 1:
        raise ValueError("PRODUCER_BURST_PROBABILITY must be between 0 and 1")

    return ProducerConfig(
        producer_id=os.getenv("PRODUCER_ID") or socket.gethostname(),
        producer_type=producer_type,
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        topic=os.getenv("KAFKA_TOPIC_RAW") or os.getenv("KAFKA_TOPIC_EVENTS", "financial-events-raw"),
        symbols=parse_symbols(os.getenv("PRODUCER_SYMBOLS")),
        rate_per_second=rate,
        scenario=os.getenv("PRODUCER_SCENARIO", "low"),
        run_duration_seconds=parse_float_env("PRODUCER_RUN_DURATION_SECONDS", 0.0),
        random_seed=parse_int_env("PRODUCER_RANDOM_SEED"),
        source=os.getenv("PRODUCER_SOURCE", "synthetic"),
        volatility=parse_float_env("PRODUCER_VOLATILITY", 0.015),
        trend_strength=parse_float_env("PRODUCER_TREND_STRENGTH", 0.002),
        burst_probability=burst_probability,
    )


class MarketEventGenerator:
    def __init__(self, config: ProducerConfig, rng: random.Random) -> None:
        self.config = config
        self.rng = rng
        self.current_prices = {symbol: BASE_PRICES.get(symbol, Decimal("100.00")) for symbol in config.symbols}
        self.trends = {
            symbol: Decimal(str(rng.uniform(-config.trend_strength, config.trend_strength)))
            for symbol in config.symbols
        }

    def next_event(self, sequence: int) -> FinancialEvent:
        symbol = self.rng.choice(self.config.symbols)
        event_time = utc_now_iso()
        producer_time = utc_now_iso()

        return FinancialEvent(
            event_id=str(uuid.uuid4()),
            producer_id=self.config.producer_id,
            symbol=symbol,
            price=self.next_price(symbol),
            quantity=self.next_quantity(),
            event_time=event_time,
            producer_time=producer_time,
            source=self.config.source,
            scenario=self.config.scenario,
            sequence=sequence,
        )

    def next_price(self, symbol: str) -> float:
        if self.config.producer_type == "trend":
            return self.next_trend_price(symbol)
        if self.config.producer_type == "burst":
            return self.next_burst_price(symbol)
        return self.next_random_price(symbol)

    def next_random_price(self, symbol: str) -> float:
        base_price = BASE_PRICES.get(symbol, Decimal("100.00"))
        variation = Decimal(str(self.rng.uniform(-self.config.volatility, self.config.volatility)))
        return self.quantize_price(base_price * (Decimal("1.0") + variation))

    def next_trend_price(self, symbol: str) -> float:
        if self.rng.random() < 0.02:
            self.trends[symbol] = Decimal(
                str(self.rng.uniform(-self.config.trend_strength, self.config.trend_strength))
            )

        noise = Decimal(str(self.rng.uniform(-self.config.volatility / 3, self.config.volatility / 3)))
        price = self.current_prices[symbol] * (Decimal("1.0") + self.trends[symbol] + noise)
        self.current_prices[symbol] = self.ensure_positive(symbol, price)
        return self.quantize_price(self.current_prices[symbol])

    def next_burst_price(self, symbol: str) -> float:
        burst_multiplier = 8 if self.rng.random() < self.config.burst_probability else 1
        variation = Decimal(
            str(self.rng.uniform(-self.config.volatility * burst_multiplier, self.config.volatility * burst_multiplier))
        )
        price = self.current_prices[symbol] * (Decimal("1.0") + variation)
        self.current_prices[symbol] = self.ensure_positive(symbol, price)
        return self.quantize_price(self.current_prices[symbol])

    def next_quantity(self) -> int:
        if self.config.producer_type == "burst" and self.rng.random() < self.config.burst_probability:
            return self.rng.randint(1_000, 10_000)
        if self.config.producer_type == "trend":
            return self.rng.randint(100, 2_000)
        return self.rng.randint(1, 1_000)

    @staticmethod
    def quantize_price(price: Decimal) -> float:
        return float(price.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

    @staticmethod
    def ensure_positive(symbol: str, price: Decimal) -> Decimal:
        if price > Decimal("0.01"):
            return price
        return BASE_PRICES.get(symbol, Decimal("100.00"))


def encode_event(event: FinancialEvent) -> bytes:
    return json.dumps(asdict(event), separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def create_kafka_producer(bootstrap_servers: str) -> KafkaProducer:
    last_error: Exception | None = None
    for attempt in range(1, 31):
        try:
            return KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                key_serializer=lambda value: value.encode("utf-8"),
                value_serializer=lambda value: value,
                acks="all",
                retries=5,
                linger_ms=10,
                max_in_flight_requests_per_connection=1,
            )
        except NoBrokersAvailable as exc:
            last_error = exc
            LOGGER.warning("Kafka unavailable at %s; retry %s/30", bootstrap_servers, attempt)
            time.sleep(2)
    raise RuntimeError(f"Kafka unavailable at {bootstrap_servers}") from last_error


def should_continue(started_at: float, duration_seconds: float, shutdown: ShutdownFlag) -> bool:
    if shutdown.requested:
        return False
    if duration_seconds <= 0:
        return True
    return (time.monotonic() - started_at) < duration_seconds


def publish_loop(config: ProducerConfig, producer: KafkaProducer, shutdown: ShutdownFlag) -> int:
    rng = random.Random(config.random_seed)
    event_generator = MarketEventGenerator(config, rng)
    sequence = 0
    sent = 0
    interval = 1.0 / config.rate_per_second
    started_at = time.monotonic()
    next_send_at = started_at
    last_log_at = started_at
    last_log_sent = 0

    while should_continue(started_at, config.run_duration_seconds, shutdown):
        now = time.monotonic()
        if now < next_send_at:
            time.sleep(min(next_send_at - now, 0.1))
            continue

        event = event_generator.next_event(sequence)
        try:
            producer.send(config.topic, key=event.symbol, value=encode_event(event))
        except KafkaError:
            LOGGER.exception("failed to publish event_id=%s sequence=%s", event.event_id, sequence)
        else:
            sent += 1
            sequence += 1

        next_send_at += interval

        if now - last_log_at >= 5:
            window_sent = sent - last_log_sent
            LOGGER.info(
                "published=%s window_rate=%.2f/s topic=%s producer_type=%s scenario=%s",
                sent,
                window_sent / (now - last_log_at),
                config.topic,
                config.producer_type,
                config.scenario,
            )
            last_log_at = now
            last_log_sent = sent

    producer.flush(timeout=30)
    return sent


def configure_logging() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def log_config(config: ProducerConfig) -> None:
    safe_config = {
        "bootstrap_servers": config.bootstrap_servers,
        "producer_id": config.producer_id,
        "producer_type": config.producer_type,
        "topic": config.topic,
        "symbols": ",".join(config.symbols),
        "rate_per_second": config.rate_per_second,
        "scenario": config.scenario,
        "run_duration_seconds": config.run_duration_seconds,
        "random_seed": config.random_seed,
        "source": config.source,
        "volatility": config.volatility,
        "trend_strength": config.trend_strength,
        "burst_probability": config.burst_probability,
    }
    LOGGER.info("starting synthetic financial producer with config=%s", safe_config)


def main() -> int:
    configure_logging()
    shutdown = ShutdownFlag()
    signal.signal(signal.SIGTERM, shutdown.stop)
    signal.signal(signal.SIGINT, shutdown.stop)

    try:
        config = load_config()
        log_config(config)
        producer = create_kafka_producer(config.bootstrap_servers)
        total_sent = publish_loop(config, producer, shutdown)
        LOGGER.info("producer stopped; total_published=%s", total_sent)
        producer.close(timeout=30)
        return 0
    except Exception:
        LOGGER.exception("producer failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
