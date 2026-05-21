# Infraestrutura

Esta pasta contém arquivos de apoio usados pelo `docker-compose.yml`.

## Estrutura

```text
infra/
  kafka/
    init-topics.sh
    README.md
  grafana/
    provisioning/
      datasources/
        timescaledb.yml
      dashboards/
        dashboards.yml
        financial-events-overview.json
  timescaledb/
    init/
      00-enable-timescaledb.sql
      01-create-financial-events.sql
      02-create-continuous-aggregates.sql
    queries/
      00-healthcheck.sql
      01-last-5-minutes.sql  .. 09-events-by-producer.sql
      10-cagg-last-5-minutes.sql
      11-cagg-last-1-hour.sql
      12-cagg-last-24-hours.sql
```

## Comandos principais

Pré-requisito: o Docker Desktop precisa estar aberto e com o engine Linux ativo.

Validar a configuração sem subir os contêineres:

```powershell
docker compose config
```

Subir a infraestrutura base:

```powershell
docker compose up -d
```

Verificar serviços:

```powershell
docker compose ps
```

Ver logs de um serviço:

```powershell
docker compose logs -f kafka
docker compose logs -f timescaledb
docker compose logs -f flink-jobmanager
docker compose logs -f grafana
```

Parar os serviços sem apagar volumes:

```powershell
docker compose down
```

Parar e apagar volumes persistidos:

```powershell
docker compose down -v
```

Subir também os placeholders de `producer` e `flink-job`:

```powershell
docker compose --profile app up -d
```

Esses placeholders existem apenas para reservar variáveis, dependências e nomes de serviços. Eles devem ser substituídos pelos agentes responsáveis pelo produtor e pelo job Flink.

## Endereços locais

| Serviço | URL ou host |
| --- | --- |
| Kafka externo | `localhost:9092` |
| TimescaleDB | `localhost:5432` |
| Flink Dashboard | `http://localhost:8081` |
| Grafana | `http://localhost:3000` |

Credenciais padrão do Grafana:

- usuário: `admin`
- senha: `admin`

Credenciais padrão do banco:

- database: `pipeline`
- usuário: `pipeline`
- senha: `pipeline`

Copie `.env.example` para `.env` se quiser alterar portas, senhas, nomes de tópicos ou parâmetros de paralelismo.

## Kafka

A estratégia de tópicos, retenção, partições e comandos de inspeção está documentada em `infra/kafka/README.md`.

## TimescaleDB

O schema temporal, scripts de inicialização e consultas de validação estão documentados em `infra/timescaledb/README.md`.

## Grafana

O datasource TimescaleDB e o dashboard operacional provisionado estão documentados em `infra/grafana/README.md`.

## Observação de validação

O arquivo `docker-compose.yml` foi validado com `docker compose config`. Para subir a stack, o Docker daemon precisa estar rodando. Se aparecer erro informando que `dockerDesktopLinuxEngine` ou `docker_engine` não foi encontrado, abra o Docker Desktop e execute `docker compose up -d` novamente.
