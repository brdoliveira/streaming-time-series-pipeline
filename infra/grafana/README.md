# Grafana

Esta pasta contém o provisionamento do Grafana para a pipeline.

## Arquivos provisionados

```text
infra/grafana/provisioning/
  datasources/
    timescaledb.yml
  dashboards/
    dashboards.yml
    financial-events-overview.json
```

## Datasource

O datasource `TimescaleDB` usa:

- tipo: PostgreSQL;
- UID: `timescaledb`;
- host interno: `timescaledb:5432`;
- database, usuário e senha vindos das variáveis `POSTGRES_DB`, `POSTGRES_USER` e `POSTGRES_PASSWORD`;
- TimescaleDB habilitado no datasource.

## Dashboard

Dashboard provisionado: **Pipeline Financeira - Visão Operacional**.

Painéis:

- preço médio por ativo;
- volume por ativo;
- latência média por cenário;
- p95 de latência;
- throughput persistido;
- total de eventos no período;
- latência média no período;
- eventos recentes.

Os painéis agregados usam `financial_event_metrics`. A tabela `financial_events` continua sendo usada para inspeção linha a linha dos eventos recentes.

## Execução

Subir o Grafana com a stack base:

```powershell
docker compose up -d grafana
```

Acessar:

```text
http://localhost:3000
```

Credenciais padrão:

- usuário: `admin`
- senha: `admin`

## Validação

Com a pipeline processando eventos, abra o dashboard **Pipeline Financeira - Visão Operacional**.

Os painéis devem exibir dados reais da tabela `financial_events`. Se os painéis aparecerem sem dados, valide primeiro:

```powershell
docker compose exec timescaledb psql -U pipeline -d pipeline -f /queries/06-recent-events.sql
```

Se a consulta retornar linhas e o dashboard não, confira se o datasource `TimescaleDB` está saudável em **Connections > Data sources**.
