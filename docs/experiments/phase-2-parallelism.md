# Fase 2: auditoria da variação de paralelismo

## Objetivo original

Comparar `FLINK_PARALLELISM` em 2, 3, 4, 6 e 8, mantendo lote JDBC em 1.000 e
intervalo em 500 ms.

## Achado da auditoria

O primeiro teste subiu a stack com paralelismo 2. Os testes seguintes usaram
`SkipStackStart`, portanto não recriaram o job Flink com os valores registrados nos
metadados. Além disso, `FLINK_TASK_SLOTS` permaneceu no padrão 3.

Embora cada pasta contenha 300.000 eventos, os valores 3, 4, 6 e 8 não podem ser tratados
como configurações efetivamente aplicadas.

## Execução isolada aproveitável

| Paralelismo | Eventos | P50 | P95 | P99 | Média | Throughput |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2 | 300.000 | 13 ms | 443 ms | 886 ms | 125,85 ms | 1.000,10 ev/s |

## Conclusão

A Fase 2 deve ser repetida após reiniciar a stack em cada configuração e disponibilizar ao
menos o mesmo número de slots solicitado pelo paralelismo. Não há evidência válida para
classificar paralelismo 6 como ótimo nem paralelismo 3 como anômalo.
