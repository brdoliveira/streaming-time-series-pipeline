# Variação de paralelismo Flink

Este experimento compara diferentes valores de `FLINK_PARALLELISM`, mantendo:

- `JDBC_BATCH_SIZE=1000`;
- `JDBC_BATCH_INTERVAL_MS=500`;
- taxa de 1.000 eventos/s;
- duração de 300 segundos.

## Isolamento e aplicação da configuração

Cada execução:

1. remove a stack e os volumes anteriores;
2. define `FLINK_PARALLELISM`;
3. define `FLINK_TASK_SLOTS` com o mesmo valor;
4. recria o cluster e o job Flink;
5. valida a contagem de eventos antes de aceitar o resultado.

Sem essas etapas, o valor gravado no metadado pode não representar o job realmente
executado.

## Execução individual

```powershell
.\scripts\experiments\Run-ParallelismVariationExperiment.ps1 `
  -FlinkParallelism 4
```

## Matriz

```powershell
.\scripts\experiments\Invoke-ParallelismVariationMatrix.ps1 `
  -ParallelismValues @(2, 3, 4, 6, 8) `
  -Repetitions 3
```

A matriz executa três repetições por padrão. Para evidência comparativa mais forte,
execute matrizes adicionais em ordens diferentes, pois a ordem crescente fixa pode
introduzir efeitos de aquecimento do ambiente.

## Análise

```powershell
.\scripts\experiments\Analyze-ParallelismVariation.ps1
```

O arquivo `audit-results.csv` contém todas as execuções. O
`consolidated-results.csv` contém apenas aquelas com reset de estado registrado e contagem
de eventos válida.

Consulte a auditoria da coleta anterior em
`docs/experiments/phase-2-parallelism.md`.
