# Fase 1: auditoria da variação JDBC

## Objetivo original

Comparar tamanhos de lote e intervalos de descarga do sink JDBC sob carga de 1.000
eventos/s durante 300 segundos.

## Achado da auditoria

A execução inicial contém 300.000 eventos, conforme esperado. Nas execuções seguintes, a
contagem cresce para 600.000, 900.000, 1.200.000 e valores superiores. As consultas foram
feitas sobre todos os registros com `scenario = 'high'`, não apenas sobre os eventos da
execução corrente.

Como consequência:

- latência e throughput representam conjuntos acumulados;
- as configurações não possuem a mesma população de eventos;
- as médias entre lote e intervalo não são comparáveis;
- não é possível afirmar, com esta coleta, qual configuração JDBC é melhor.

## Execução isolada aproveitável

| Lote | Intervalo | Eventos | P50 | P95 | P99 | Média | Throughput |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 500 | 1.000 ms | 300.000 | 11 ms | 491 ms | 3.012 ms | 161,10 ms | 999,67 ev/s |

Essa linha é uma observação de referência, não uma comparação.

## Conclusão

A Fase 1 deve ser repetida com estado isolado e múltiplas repetições. As afirmações
anteriores de redução de 50% e de configuração ótima não são sustentadas pelos dados
auditados.
