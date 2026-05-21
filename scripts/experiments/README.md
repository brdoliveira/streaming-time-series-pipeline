# Experimentos e Validação

Esta pasta contém scripts para executar cenários de carga, coletar métricas e gerar resumo comparativo.

## Pré-requisitos

- Docker Desktop aberto e com engine Linux ativo.
- Executar os comandos a partir da raiz do projeto `projeto`.

## Cenários padrão

| Cenário | Taxa | Duração |
| --- | --- | --- |
| `low` | 10 eventos/s | 300 s |
| `medium` | 100 eventos/s | 600 s |
| `high` | 1000 eventos/s | 600 s |

Os valores podem ser sobrescritos por parâmetro.

## Rodar um cenário

```powershell
.\scripts\experiments\Run-Scenario.ps1 -Scenario low
```

Com taxa e duração customizadas:

```powershell
.\scripts\experiments\Run-Scenario.ps1 -Scenario medium -RatePerSecond 200 -DurationSeconds 120
```

Com múltiplos produtores em paralelo:

```powershell
.\scripts\experiments\Run-Scenario.ps1 -Scenario medium -RatePerSecond 100 -DurationSeconds 120 -ProducerCount 3
```

Nesse exemplo, cada produtor publica 100 eventos/s. A carga total esperada fica próxima de 300 eventos/s.

Com um perfil de producer específico:

```powershell
.\scripts\experiments\Run-Scenario.ps1 -Scenario high -ProducerType burst -DurationSeconds 120
```

Perfis disponíveis:

- `random`: variações independentes em torno do preço base;
- `trend`: série com drift por ativo;
- `burst`: rajadas de preço e volume para stress.

O script:

1. sobe Kafka, TimescaleDB, Flink, Grafana e o job Flink;
2. executa o produtor pelo tempo definido;
3. coleta consultas SQL de validação;
4. coleta amostras de `docker stats --no-stream` durante o cenário;
5. grava artefatos em `results/<timestamp>-<scenario>/`.

O intervalo de coleta de recursos é configurável:

```powershell
.\scripts\experiments\Run-Scenario.ps1 -Scenario high -StatsIntervalSeconds 5
```

## Rodar todos os cenários

```powershell
.\scripts\experiments\Run-All-Scenarios.ps1
```

Rodar todos os cenários com três produtores:

```powershell
.\scripts\experiments\Run-All-Scenarios.ps1 -ProducerCount 3
```

Rodar todos os cenários usando o producer de tendência:

```powershell
.\scripts\experiments\Run-All-Scenarios.ps1 -ProducerType trend
```

## Gerar resumo comparativo

```powershell
.\scripts\experiments\Collect-Summary.ps1
```

O resumo será salvo em `results/summary-<timestamp>/`.

Arquivos comparativos gerados:

| Arquivo | Conteúdo |
| --- | --- |
| `latency-by-scenario.csv` | avg, stddev, p50, p95, max de latência por cenário |
| `throughput-by-scenario.csv` | eventos/s efetivo por cenário |
| `minute-by-minute.csv` | série temporal minuto a minuto por cenário |
| `aggregated-metrics.csv` | janelas de 10 s vindas do Flink |
| `query-response-times.csv` | tempo de resposta (raw vs cagg) por janela e cenário |
| `resource-usage-summary.csv` | média de CPU% e memória% por container por cenário |
| `docker-stats.txt` | snapshot final dos containers |

## Validação manual rápida

```powershell
docker compose exec timescaledb psql -U pipeline -d pipeline -f /queries/00-healthcheck.sql
docker compose exec timescaledb psql -U pipeline -d pipeline -f /queries/04-latency-by-scenario.sql
docker compose exec timescaledb psql -U pipeline -d pipeline -f /queries/05-throughput-per-second.sql
```
