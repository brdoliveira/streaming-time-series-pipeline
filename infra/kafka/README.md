# Kafka e Tópicos

Esta pasta contém a configuração operacional da camada Kafka.

## Estratégia de tópicos

| Tópico | Variável | Partições | Retenção | Uso |
| --- | --- | --- | --- | --- |
| `financial-events-raw` | `KAFKA_TOPIC_RAW` | `KAFKA_TOPIC_PARTITIONS` | `KAFKA_RETENTION_MS` | Eventos financeiros brutos publicados pelo produtor |
| `financial-events` | `KAFKA_TOPIC_EVENTS` | `KAFKA_TOPIC_PARTITIONS` | `KAFKA_RETENTION_MS` | Tópico legado mantido para compatibilidade |
| `financial-events-processed` | `KAFKA_TOPIC_PROCESSED` | `KAFKA_TOPIC_PARTITIONS` | `KAFKA_RETENTION_MS` | Eventos válidos e enriquecidos pelo Flink |
| `financial-events-invalid` | `KAFKA_TOPIC_ERRORS` | `KAFKA_TOPIC_PARTITIONS` | `KAFKA_RETENTION_MS` | Eventos inválidos ou rejeitados pelo processamento |
| `pipeline-metrics` | `KAFKA_TOPIC_METRICS` | `KAFKA_METRICS_TOPIC_PARTITIONS` | `KAFKA_RETENTION_MS` | Métricas operacionais opcionais |

Padrões:

- `KAFKA_TOPIC_PARTITIONS=3`
- `KAFKA_METRICS_TOPIC_PARTITIONS=1`
- `KAFKA_REPLICATION_FACTOR=1`
- `KAFKA_RETENTION_MS=86400000`
- `KAFKA_CLEANUP_POLICY=delete`

Em ambiente local, o fator de replicação fica em `1`. Em um cluster real, ele deve ser aumentado junto com o número de brokers.

## Contratos compartilhados

Produtor e Flink devem usar os mesmos nomes:

- `KAFKA_BOOTSTRAP_SERVERS=kafka:9092`
- `KAFKA_TOPIC_RAW=financial-events-raw`
- `KAFKA_TOPIC_PROCESSED=financial-events-processed`
- `KAFKA_TOPIC_ERRORS=financial-events-invalid`
- `KAFKA_TOPIC_METRICS=pipeline-metrics`
- `FLINK_KAFKA_GROUP_ID=financial-events-flink`

A chave das mensagens do tópico `financial-events-raw` deve ser `symbol`. O valor deve ser JSON UTF-8 com os campos: `event_id`, `producer_id`, `symbol`, `price`, `quantity`, `event_time`, `producer_time`, `source`, `scenario`, `sequence`.

## Inicialização

O serviço `kafka-init` executa `infra/kafka/init-topics.sh` automaticamente após o Kafka ficar saudável.

O script:

1. cria os tópicos se ainda não existirem;
2. aplica `retention.ms`;
3. aplica `cleanup.policy`;
4. lista e descreve os tópicos criados.

## Comandos de inspeção

Listar tópicos:

```powershell
docker compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --list
```

Descrever o tópico principal:

```powershell
docker compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic financial-events-raw
```

Ver configurações de retenção:

```powershell
docker compose exec kafka kafka-configs.sh --bootstrap-server kafka:9092 --entity-type topics --entity-name financial-events-raw --describe
```

Consumir mensagens do começo:

```powershell
docker compose exec kafka kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic financial-events-raw --from-beginning --property print.key=true --property key.separator=" | " --max-messages 5
```

Produzir uma mensagem manual com chave `PETR4`:

```powershell
docker compose exec -T kafka kafka-console-producer.sh --bootstrap-server kafka:9092 --topic financial-events-raw --property parse.key=true --property key.separator="|"
```

Depois cole uma linha neste formato:

```text
PETR4|{"event_id":"00000000-0000-0000-0000-000000000001","symbol":"PETR4","price":38.42,"quantity":100,"event_time":"2026-04-28T14:00:00.123Z","producer_time":"2026-04-28T14:00:00.130Z","source":"manual","scenario":"low","sequence":1}
```

Ver grupos de consumidores:

```powershell
docker compose exec kafka kafka-consumer-groups.sh --bootstrap-server kafka:9092 --list
```

Descrever o grupo do Flink:

```powershell
docker compose exec kafka kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group financial-events-flink
```

## Critério de aceite

Com Docker ativo, estes comandos devem funcionar:

```powershell
docker compose up -d kafka kafka-init
docker compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --list
docker compose --profile app up -d producer
docker compose exec kafka kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic financial-events-raw --from-beginning --max-messages 5
```

O último comando deve retornar eventos JSON válidos publicados pelo produtor.
