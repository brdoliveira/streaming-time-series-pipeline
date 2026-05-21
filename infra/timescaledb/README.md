# TimescaleDB

Esta pasta contém os scripts de inicialização, schema temporal e consultas de validação do banco.

## Inicialização automática

Os arquivos em `infra/timescaledb/init` são montados em `/docker-entrypoint-initdb.d` pelo serviço `timescaledb` no `docker-compose.yml`.

Ordem dos scripts:

1. `00-enable-timescaledb.sql`: habilita a extensão TimescaleDB.
2. `01-create-financial-events.sql`: cria a tabela `financial_events`, converte em hypertable e cria índices. Cria também a tabela `financial_event_metrics`.
3. `02-create-continuous-aggregates.sql`: cria as continuous aggregates `cagg_events_1min` e `cagg_events_15min` com políticas de refresh automático.

Importante: scripts em `/docker-entrypoint-initdb.d` rodam apenas na primeira criação do volume do PostgreSQL. Para recriar o banco do zero durante desenvolvimento:

```powershell
docker compose down -v
docker compose up -d timescaledb
```

## Tabela principal

Tabela: `financial_events`

Colunas principais:

- `event_id`
- `producer_id`
- `symbol`
- `price`
- `quantity`
- `event_time`
- `producer_time`
- `processing_time`
- `source`
- `scenario`
- `sequence`
- `ingestion_latency_ms`
- `event_lag_ms`
- `created_at`

`event_time` é a coluna temporal usada pela hypertable.

Tabela agregada: `financial_event_metrics`

`bucket_start` é a coluna temporal usada pela hypertable agregada. Cada linha representa uma janela temporal por `symbol` e `scenario`, com preço médio, volume, latência média, p50 e p95.

## Índices

| Índice | Uso |
| --- | --- |
| `idx_financial_events_event_id_time` | Evitar duplicidade prática por evento dentro do particionamento temporal |
| `idx_financial_events_producer_time` | Comparar volume e latência por produtor |
| `idx_financial_events_symbol_time` | Consultas por ativo e janela temporal |
| `idx_financial_events_scenario_time` | Comparações por cenário experimental |
| `idx_financial_events_processing_time` | Throughput por tempo de processamento |
| `idx_financial_events_created_at` | Inspeção dos registros recém-inseridos |

## Consultas de validação

Os arquivos em `infra/timescaledb/queries` podem ser executados com `psql`.

Executar todas as validações principais:

```powershell
docker compose exec timescaledb psql -U pipeline -d pipeline -f /queries/00-healthcheck.sql
docker compose exec timescaledb psql -U pipeline -d pipeline -f /queries/01-last-5-minutes.sql
docker compose exec timescaledb psql -U pipeline -d pipeline -f /queries/02-last-1-hour.sql
docker compose exec timescaledb psql -U pipeline -d pipeline -f /queries/03-last-24-hours.sql
docker compose exec timescaledb psql -U pipeline -d pipeline -f /queries/04-latency-by-scenario.sql
docker compose exec timescaledb psql -U pipeline -d pipeline -f /queries/05-throughput-per-second.sql
docker compose exec timescaledb psql -U pipeline -d pipeline -f /queries/07-aggregated-metrics-recent.sql
docker compose exec timescaledb psql -U pipeline -d pipeline -f /queries/08-aggregated-throughput.sql
```

O diretório `queries` é montado no contêiner em `/queries`.

## Continuous Aggregates

Duas views materializadas são criadas automaticamente sobre `financial_events`:

| View | Bucket | Refresh | Descrição |
| --- | --- | --- | --- |
| `cagg_events_1min` | 1 minuto | 30 s | Agrega count, preço, quantidade, latência média e stddev por símbolo e cenário |
| `cagg_events_15min` | 15 minutos | 2 min | Hierárquico sobre `cagg_events_1min`; usado pela query de 24 horas |

Refresh manual (necessário antes de medir tempo de resposta das caggs):

```powershell
docker compose exec timescaledb psql -U pipeline -d pipeline -c "CALL refresh_continuous_aggregate('cagg_events_1min', now() - interval '25 hours', now());"
docker compose exec timescaledb psql -U pipeline -d pipeline -c "CALL refresh_continuous_aggregate('cagg_events_15min', now() - interval '26 hours', now());"
```

Queries sobre continuous aggregates (para comparação com raw):

```powershell
docker compose exec timescaledb psql -U pipeline -d pipeline -f /queries/10-cagg-last-5-minutes.sql
docker compose exec timescaledb psql -U pipeline -d pipeline -f /queries/11-cagg-last-1-hour.sql
docker compose exec timescaledb psql -U pipeline -d pipeline -f /queries/12-cagg-last-24-hours.sql
```

## Critério de aceite

Com a stack em execução, este comando deve mostrar que a extensão existe, a tabela existe e a tabela é hypertable:

```powershell
docker compose exec timescaledb psql -U pipeline -d pipeline -f /queries/00-healthcheck.sql
```

Depois que produtor e Flink estiverem ativos, as consultas temporais devem retornar dados agregados.
