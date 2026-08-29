# Auditoria dos experimentos de configuração

Esta pasta organiza os resultados exploratórios executados em 9 e 10 de junho de 2026.
Os dados brutos permanecem em `scripts/experiments/results-*` e são ignorados pelo Git.

## Situação dos resultados

| Fase | Variável | Situação | Motivo |
| --- | --- | --- | --- |
| 1 | Lote e intervalo JDBC | Inconclusiva | O banco acumulou eventos entre execuções, tornando as amostras não comparáveis |
| 2 | Paralelismo Flink | Inconclusiva | Após o primeiro teste, o job não foi reiniciado com os novos valores solicitados |

Em cada fase há uma execução individual aproveitável como observação isolada:

- Fase 1: lote 500, intervalo 1.000 ms, 300.000 eventos, P95 de 491 ms.
- Fase 2: paralelismo 2, 300.000 eventos, P95 de 443 ms.

Essas observações não permitem concluir causalidade nem escolher uma configuração ótima.

## Arquivos

- `phase-1-jdbc.md`: auditoria da variação JDBC.
- `phase-2-parallelism.md`: auditoria da variação de paralelismo.
- `data/jdbc-runs-audit.csv`: inventário das execuções JDBC.
- `data/parallelism-runs-audit.csv`: inventário das execuções de paralelismo.

## Protocolo corrigido

Os runners em `scripts/experiments/` agora:

1. removem a stack e os volumes antes de cada execução;
2. recriam os serviços com a configuração solicitada;
3. igualam `FLINK_TASK_SLOTS` ao paralelismo testado;
4. aguardam o processamento do volume esperado;
5. registram a validação da contagem no `metadata.json`.

O reset de volumes é intencional. Ele evita reprocessamento de mensagens antigas do Kafka e
acúmulo de registros no TimescaleDB, garantindo independência entre execuções.

## Próxima coleta

Para produzir evidência comparativa, execute ao menos três repetições por configuração.
Depois, use os scripts de análise para consolidar somente execuções cuja contagem tenha sido
validada.

## Benchmark equivalente de consultas

O ganho exploratorio reportado anteriormente comparava consultas com granularidades
diferentes. Para uma comparacao controlada, use:

```powershell
.\scripts\experiments\Measure-EquivalentQueryBenchmark.ps1 `
  -Repetitions 10 `
  -WarmupRuns 2 `
  -WindowHours 24
```

O runner alinha a janela em buckets completos de 15 minutos, atualiza as CAGGs, exige
igualdade entre as linhas retornadas pelos caminhos raw e CAGG e somente entao mede as
consultas. As repeticoes alternam a ordem de execucao e sao gravadas individualmente em
CSV; os aquecimentos nao entram nas medicoes. Os arquivos ficam em
`scripts/experiments/results-query-benchmark/`.

## Validacao final executavel

O registro de smoke ponta a ponta, coleta real de recursos, benchmark equivalente e
recuperacao por checkpoint esta em
[`final-validation-2026-08-29.md`](final-validation-2026-08-29.md). Os numeros desse
registro sao observacoes do ambiente local e preservam as ressalvas de causalidade da
auditoria experimental.
