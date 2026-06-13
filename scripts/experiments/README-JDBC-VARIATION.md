# Variação de configuração JDBC

Este conjunto de scripts compara `JDBC_BATCH_SIZE` e
`JDBC_BATCH_INTERVAL_MS` sob uma carga controlada.

## Isolamento experimental

Cada execução remove a stack e os volumes antes de iniciar. Esse comportamento evita:

- eventos antigos retidos no Kafka;
- registros acumulados no TimescaleDB;
- reprocessamento de mensagens de uma configuração anterior;
- comparação entre populações de tamanhos diferentes.

O reset é obrigatório para que a matriz produza resultados comparáveis.

## Execução individual

Execute a partir da raiz do projeto:

```powershell
.\scripts\experiments\Run-JdbcVariationExperiment.ps1 `
  -JdbcBatchSize 500 `
  -JdbcBatchIntervalMs 1000
```

Parâmetros principais:

| Parâmetro | Padrão | Descrição |
| --- | ---: | --- |
| `JdbcBatchSize` | 500 | Registros por lote |
| `JdbcBatchIntervalMs` | 1.000 | Intervalo máximo entre descargas |
| `RatePerSecond` | 1.000 | Taxa do produtor |
| `DurationSeconds` | 300 | Duração da produção |
| `StatsIntervalSeconds` | 5 | Intervalo de coleta de recursos |
| `OutputRoot` | `results-jdbc-variation` | Diretório dos artefatos locais |

## Matriz

Matriz reduzida:

```powershell
.\scripts\experiments\Invoke-JdbcVariationMatrix.ps1 `
  -Repetitions 3 `
  -BatchSizes @(100, 500, 1000) `
  -BatchIntervals @(500, 1000)
```

Matriz completa:

```powershell
.\scripts\experiments\Invoke-JdbcVariationMatrix.ps1
```

Cada configuração é executada em uma stack nova. O tempo total inclui inicialização dos
serviços e estabilização entre testes.

## Análise

```powershell
.\scripts\experiments\Analyze-JdbcVariation.ps1
```

O analisador:

- compara a contagem observada com `taxa x duração`;
- exige registro de reset de estado;
- gera `audit-results.csv` com todas as execuções;
- exclui execuções com contagem incompatível;
- gera os consolidados apenas com dados válidos.

Para investigar dados rejeitados:

```powershell
.\scripts\experiments\Analyze-JdbcVariation.ps1 -IncludeInvalid
```

## Interpretação

Uma configuração só deve ser considerada melhor após múltiplas repetições válidas. Compare
P95, P99, throughput e variabilidade; não escolha uma configuração com base em uma única
execução.

Consulte a auditoria das coletas anteriores em `docs/experiments/phase-1-jdbc.md`.
