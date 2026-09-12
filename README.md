# Pipeline de Ingestão de Dados Temporais

Pipeline de baixa latência para eventos financeiros simulados, implementada com Apache Kafka, Apache Flink, TimescaleDB e Grafana em ambiente Docker Compose local.

## Arquitetura

| Camada | Tecnologia | Papel |
| --- | --- | --- |
| Fonte de dados | Produtor Python | Gera eventos financeiros sintéticos com timestamp, símbolo, preço e quantidade |
| Ingestão | Apache Kafka 3.9.0 | Recebe e distribui eventos em tópicos particionados por símbolo |
| Processamento | Apache Flink 1.19 | Valida, enriquece, calcula latência e agrega eventos em janelas de tempo |
| Persistência | TimescaleDB 2.16 | Armazena séries temporais em hypertables com continuous aggregates |
| Visualização | Grafana 11 | Exibe painéis operacionais provisionados automaticamente |

## Pré-requisitos

- Docker Desktop aberto com engine Linux ativo
- PowerShell 5.1+
- Java 17 para compilar e testar fora do Docker
- Python 3.11 para executar a suíte do produtor
- Executar todos os comandos a partir da raiz `projeto/`

Não é necessário instalar Maven globalmente. O Maven Wrapper oficial baixa e reutiliza
a versão 3.9.9 declarada em `.mvn/wrapper/maven-wrapper.properties`, validando o SHA-256
da distribuição antes de executá-la.

## Quick start

```powershell
# 1. Copiar variáveis de ambiente (opcional — os padrões já funcionam)
Copy-Item .env.example .env

# 2. Subir infraestrutura base
docker compose up -d

# 3. Subir aplicação (producer + flink-job) e executar cenário low
.\scripts\experiments\Run-Scenario.ps1 -Scenario low

# 4. Abrir Grafana
Start-Process http://localhost:3000   # admin / admin
```

## Build e testes reproduzíveis

No Windows:

```powershell
# Java/Flink (baixa o Maven fixado na primeira execução)
.\mvnw.cmd --batch-mode --no-transfer-progress -f flink-job/pom.xml clean test package

# Python/produtor
py -3.11 -m pip install -r producers/requirements.txt
py -3.11 -m unittest discover -s producers/tests -p "test_*.py" -v

# Configuração do ambiente
docker compose config --quiet
```

Em Linux ou macOS, execute o build Java com
`sh ./mvnw --batch-mode --no-transfer-progress -f flink-job/pom.xml clean test package`.

A automação em `.github/workflows/ci.yml` repete essas provas em ambiente limpo,
valida os JSON versionados com Node.js, confere o Compose e executa o smoke test.

## Smoke test determinístico

Com a stack e o job Flink já saudáveis, o teste publica um evento válido e um inválido
identificados por UUIDs exclusivos. Ele exige exatamente um registro correspondente no
TimescaleDB, nenhuma persistência do inválido e a presença da rejeição no tópico Kafka:

```powershell
.\scripts\Smoke-Test.ps1
```

Para construir e iniciar somente os componentes necessários antes da prova:

```powershell
.\scripts\Smoke-Test.ps1 -StartStack
```

Em ambiente descartável (como a CI), `-Cleanup` remove também os volumes depois da
execução. Não use essa opção sobre resultados locais que precisem ser preservados:

```powershell
.\scripts\Smoke-Test.ps1 -StartStack -Cleanup
```

Se `.env` alterar nomes de tópicos ou credenciais, informe os valores correspondentes
pelos parâmetros `-KafkaRawTopic`, `-KafkaInvalidTopic`, `-DatabaseName` e
`-DatabaseUser`.

## Serviços e portas

| Serviço | URL / host local |
| --- | --- |
| Grafana | `http://localhost:3000` |
| Flink Dashboard | `http://localhost:8081` |
| Kafka | `localhost:9092` |
| TimescaleDB | `localhost:5432` |

Credenciais padrão: Grafana `admin/admin` · TimescaleDB `pipeline/pipeline`.

## Fluxo de dados

```text
Produtor Python
  → financial-events-raw (Kafka)
  → Flink: valida · calcula ingestion_latency_ms · event_lag_ms
  → financial-events-processed (Kafka)
  → financial_events (TimescaleDB hypertable)
  → financial_event_metrics (TimescaleDB — janelas de 10 s)
  → cagg_events_1min / cagg_events_15min (continuous aggregates)
  → Grafana
```

Eventos que falham na validação são enviados para `financial-events-invalid`.

## Tipos de produtor

| Tipo | `PRODUCER_TYPE` | Comportamento |
| --- | --- | --- |
| Aleatório | `random` | Variações independentes em torno do preço base |
| Tendência | `trend` | Deriva gradual por ativo simulando caminhada temporal |
| Rajada | `burst` | Picos de preço e volume em intervalos aleatórios |

## Experimentos

```powershell
# Cenário único
.\scripts\experiments\Run-Scenario.ps1 -Scenario low
.\scripts\experiments\Run-Scenario.ps1 -Scenario medium -ProducerType trend
.\scripts\experiments\Run-Scenario.ps1 -Scenario high -ProducerCount 3

# Todos os cenários em sequência
.\scripts\experiments\Run-All-Scenarios.ps1

# Série isolada para repetição experimental
.\scripts\experiments\Run-Isolated-Repetition.ps1 -Repetition 1

# Gerar resumo comparativo após os experimentos
.\scripts\experiments\Collect-Summary.ps1
```

Resultados salvos em `results/<timestamp>-<scenario>/`. O resumo comparativo fica em
`results/summary-<timestamp>/`. As três séries finais de 7 de setembro de 2026 foram
preservadas em `scripts/experiments/results-repeated/`, junto com uma auditoria de
contagem, consultas e coleta de recursos.

Os experimentos exploratórios de configuração JDBC e paralelismo ficam documentados em
[`docs/experiments/`](docs/experiments/README.md). Os artefatos brutos permanecem locais e
não são versionados; os relatórios consolidados registram também as ameaças à validade.

> **Validade experimental:** as coletas originais das variações JDBC e de paralelismo
> foram auditadas como inconclusivas. Elas não comprovam configuração superior. Os runners
> foram corrigidos para reiniciar volumes e aplicar a configuração a cada execução, mas a
> matriz comparativa ainda precisa ser repetida ao menos três vezes por configuração.

## Métricas coletadas

| Métrica | Origem |
| --- | --- |
| Latência até o início do processamento Flink (avg, stddev, p50, p95, max) | `financial_events.ingestion_latency_ms` |
| Throughput efetivo (eventos/s) | `financial_events.processing_time` |
| Uso de CPU e memória por cenário | `docker stats` amostrado a cada 10 s |
| Tempo de resposta das consultas temporais (raw vs cagg) | `Measure-Command` sobre psql |

## Continuous Aggregates

O TimescaleDB mantém duas views materializadas sobre `financial_events`:

- `cagg_events_1min` — buckets de 1 minuto, refresh automático a cada 30 s
- `cagg_events_15min` — buckets de 15 minutos, construída hierarquicamente sobre `cagg_events_1min`, refresh a cada 2 min

Cada experimento mede o tempo de resposta das queries de janela temporal usando dados brutos e dados das continuous aggregates, permitindo comparação direta.

## Estrutura do projeto

```text
projeto/
  docker-compose.yml          configuração de todos os serviços
  .env.example                variáveis de ambiente com padrões
  diagrama.md                 diagrama da arquitetura (Mermaid)
  producers/                  produtor Python de eventos financeiros
  flink-job/                  job Flink (Java/Maven)
  infra/
    kafka/                    script de criação de tópicos
    timescaledb/
      init/                   scripts SQL de inicialização
      queries/                consultas de validação e análise
    grafana/
      provisioning/           datasource e dashboard provisionados
  scripts/
    experiments/              scripts de experimento e coleta de métricas
```

## Recriar o banco do zero

Necessário ao alterar scripts de inicialização (ex.: após adicionar continuous aggregates):

```powershell
docker compose down -v
docker compose up -d
```
