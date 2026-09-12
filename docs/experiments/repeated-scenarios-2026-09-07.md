# Repetições dos cenários finais — 7 de setembro de 2026

Este registro documenta as três séries usadas na versão final do TCC. Ele complementa,
sem substituir, os resultados originais de maio e as auditorias inconclusivas das
variações JDBC e de paralelismo.

## Proveniência

- Repositório: `https://github.com/brdoliveira/streaming-time-series-pipeline`.
- Código do runner e do Compose: commit `47174480be494d8a311f03e9c17c2a1190908d85`.
- Host: Dell Vostro 3470, Intel Core i7-8700, 15,83 GB de RAM e Windows 11.
- Docker Desktop/Engine 28.0.4 sobre WSL2, com 12 processadores lógicos e 7,67 GiB.
- Dados brutos: `scripts/experiments/results-repeated/`.
- Manifesto: `data/repeated-experiments-2026-09-07.json`.

O commit foi produzido depois da coleta para registrar exatamente as adaptações locais
usadas pelo runner. Nenhum arquivo bruto foi reescrito; a auditoria posterior apenas os
lê e calcula contagens e hashes.

## Desenho executado

Foram realizadas três séries independentes. Antes de cada série, o runner criou nomes e
volumes exclusivos e vazios. Dentro de cada série, baixa, média e alta carga foram
executadas nessa ordem, mantendo a stack ativa entre cenários.

| Cenário | Taxa | Duração por run | Runs | Eventos por run | Total |
| --- | ---: | ---: | ---: | ---: | ---: |
| Baixa carga | 10 ev/s | 300 s | 3 | 3.000 | 9.000 |
| Carga média | 100 ev/s | 600 s | 3 | 60.000 | 180.000 |
| Alta carga | 1.000 ev/s | 600 s | 3 | 600.000 | 1.800.000 |

## Resultado auditado

O script `scripts/experiments/audit_repeated_results.py` encontrou nove runs, três por
cenário. Em todos eles, a contagem obtida pela consulta de latência coincidiu com a
contagem da consulta de throughput e com `taxa × duração`. Cada run contém as seis
medições previstas de consulta, e a coleta de recursos terminou com status `success`.

| Cenário | Média das médias | DP entre médias | P95 mediano | Intervalo do P95 | Throughput médio |
| --- | ---: | ---: | ---: | ---: | ---: |
| Baixa carga | 45,64 ms | 39,85 ms | 71,05 ms | 15–426,40 ms | 10,00 ev/s |
| Carga média | 81,11 ms | 125,93 ms | 13 ms | 13–1.298,20 ms | 100,01 ev/s |
| Alta carga | 84,03 ms | 40,83 ms | 246 ms | 241–536 ms | 999,98 ev/s |

## Limites de interpretação

- A ordem dos cenários não foi randomizada.
- Três repetições descrevem variabilidade, mas não sustentam generalização estatística.
- `processing_time` é registrado antes do `JdbcSink`; a latência não termina na confirmação
  da escrita no TimescaleDB.
- CPU e memória representam a soma dos contêineres ativos, não métricas por operador.
- As consultas raw e CAGG não são semanticamente equivalentes nas medições principais.
- As matrizes JDBC e de paralelismo continuam inconclusivas e não fundamentam uma
  configuração ótima.

## Como verificar

Na raiz do repositório:

```powershell
python .\scripts\experiments\audit_repeated_results.py
```

O comando grava `data/repeated-results-audit-2026-09-12.json` e retorna código diferente
de zero se faltar um run, se uma contagem divergir ou se a coleta de recursos estiver
incompleta.
