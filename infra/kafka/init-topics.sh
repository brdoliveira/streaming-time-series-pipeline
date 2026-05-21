#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
RAW_TOPIC="${KAFKA_TOPIC_RAW:-financial-events-raw}"
LEGACY_EVENTS_TOPIC="${KAFKA_TOPIC_EVENTS:-financial-events}"
PROCESSED_TOPIC="${KAFKA_TOPIC_PROCESSED:-financial-events-processed}"
ERRORS_TOPIC="${KAFKA_TOPIC_ERRORS:-financial-events-invalid}"
METRICS_TOPIC="${KAFKA_TOPIC_METRICS:-pipeline-metrics}"
TOPIC_PARTITIONS="${KAFKA_TOPIC_PARTITIONS:-3}"
METRICS_PARTITIONS="${KAFKA_METRICS_TOPIC_PARTITIONS:-1}"
REPLICATION_FACTOR="${KAFKA_REPLICATION_FACTOR:-1}"
RETENTION_MS="${KAFKA_RETENTION_MS:-86400000}"
CLEANUP_POLICY="${KAFKA_CLEANUP_POLICY:-delete}"

create_topic() {
  local topic="$1"
  local partitions="$2"

  kafka-topics.sh \
    --bootstrap-server "${BOOTSTRAP_SERVERS}" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions "${partitions}" \
    --replication-factor "${REPLICATION_FACTOR}" \
    --config "retention.ms=${RETENTION_MS}" \
    --config "cleanup.policy=${CLEANUP_POLICY}"

  kafka-configs.sh \
    --bootstrap-server "${BOOTSTRAP_SERVERS}" \
    --entity-type topics \
    --entity-name "${topic}" \
    --alter \
    --add-config "retention.ms=${RETENTION_MS},cleanup.policy=${CLEANUP_POLICY}"
}

echo "Creating Kafka topics on ${BOOTSTRAP_SERVERS}"
create_topic "${RAW_TOPIC}" "${TOPIC_PARTITIONS}"
create_topic "${LEGACY_EVENTS_TOPIC}" "${TOPIC_PARTITIONS}"
create_topic "${PROCESSED_TOPIC}" "${TOPIC_PARTITIONS}"
create_topic "${ERRORS_TOPIC}" "${TOPIC_PARTITIONS}"
create_topic "${METRICS_TOPIC}" "${METRICS_PARTITIONS}"

echo "Configured topics:"
kafka-topics.sh --bootstrap-server "${BOOTSTRAP_SERVERS}" --list

echo "Topic details:"
kafka-topics.sh --bootstrap-server "${BOOTSTRAP_SERVERS}" --describe --topic "${RAW_TOPIC}"
kafka-topics.sh --bootstrap-server "${BOOTSTRAP_SERVERS}" --describe --topic "${LEGACY_EVENTS_TOPIC}"
kafka-topics.sh --bootstrap-server "${BOOTSTRAP_SERVERS}" --describe --topic "${PROCESSED_TOPIC}"
kafka-topics.sh --bootstrap-server "${BOOTSTRAP_SERVERS}" --describe --topic "${ERRORS_TOPIC}"
kafka-topics.sh --bootstrap-server "${BOOTSTRAP_SERVERS}" --describe --topic "${METRICS_TOPIC}"
