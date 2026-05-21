# Job Flink de Processamento

Este componente implementa o Agente 4: Processamento Flink.

## Função

O job consome eventos JSON do tópico `financial-events-raw`, valida o contrato, calcula métricas de latência, publica eventos válidos enriquecidos em `financial-events-processed`, persiste eventos válidos na tabela `financial_events` e grava agregações por janela em `financial_event_metrics`.

Eventos inválidos são enviados para o tópico `financial-events-invalid` com `valid=false`, `validation_error` e `processing_time`.

## Fluxo

```text
Kafka financial-events-raw
  -> Flink KafkaSource
  -> validação e enriquecimento
  -> Kafka financial-events-processed
  -> JdbcSink TimescaleDB financial_events
  -> janelas de processamento TimescaleDB financial_event_metrics
  -> side output inválido para Kafka financial-events-invalid
```

## Variáveis de ambiente

| Variável | Padrão | Uso |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Broker Kafka |
| `KAFKA_TOPIC_RAW` | `financial-events-raw` | Tópico bruto de entrada |
| `KAFKA_TOPIC_PROCESSED` | `financial-events-processed` | Tópico de eventos válidos enriquecidos |
| `KAFKA_TOPIC_ERRORS` | `financial-events-invalid` | Tópico de eventos inválidos |
| `FLINK_KAFKA_GROUP_ID` | `financial-events-flink` | Grupo consumidor |
| `POSTGRES_HOST` | `timescaledb` | Host do TimescaleDB |
| `POSTGRES_PORT` | `5432` | Porta do TimescaleDB |
| `POSTGRES_DB` | `pipeline` | Banco |
| `POSTGRES_USER` | `pipeline` | Usuário |
| `POSTGRES_PASSWORD` | `pipeline` | Senha |
| `FLINK_PARALLELISM` | `3` | Paralelismo do job |
| `FLINK_CHECKPOINT_INTERVAL_MS` | `30000` | Intervalo de checkpoint |
| `JDBC_BATCH_SIZE` | `500` | Tamanho do lote de escrita |
| `JDBC_BATCH_INTERVAL_MS` | `1000` | Intervalo máximo do lote |
| `JDBC_MAX_RETRIES` | `3` | Tentativas de escrita JDBC |
| `METRICS_WINDOW_SECONDS` | `10` | Tamanho da janela de agregação |

## Execução

Subir infraestrutura, produtor e job:

```powershell
docker compose --profile app up -d
```

Subir apenas o job depois da infraestrutura:

```powershell
docker compose --profile app up -d flink-job
```

Ver jobs no Flink:

```powershell
docker compose exec flink-jobmanager flink list
```

Ver eventos persistidos:

```powershell
docker compose exec timescaledb psql -U pipeline -d pipeline -c "SELECT event_id, symbol, price, ingestion_latency_ms, event_lag_ms FROM financial_events ORDER BY created_at DESC LIMIT 5;"
```

Ver métricas agregadas:

```powershell
docker compose exec timescaledb psql -U pipeline -d pipeline -f /queries/07-aggregated-metrics-recent.sql
```

Ver eventos inválidos:

```powershell
docker compose exec kafka kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic financial-events-invalid --from-beginning --max-messages 5
```

## Critério de aceite

Após o produtor publicar eventos, a consulta abaixo deve retornar linhas:

```powershell
docker compose exec timescaledb psql -U pipeline -d pipeline -c "SELECT count(*) FROM financial_events;"
```
