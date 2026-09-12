# Resumo Comparativo

Gerado em: 2026-09-07T22:35:38.9993218Z

Banco: pipeline
UsuÃ¡rio: pipeline

Arquivos:

- latency-by-scenario.csv       (avg, stddev, p50, p95, max por cenÃ¡rio)
- throughput-by-scenario.csv    (eventos/s efetivo por cenÃ¡rio)
- minute-by-minute.csv          (sÃ©rie temporal minuto a minuto)
- aggregated-metrics.csv        (janelas de 10 s do Flink)
- query-response-times.csv      (tempo de resposta das consultas 5 min / 1 h / 24 h por cenÃ¡rio)
- resource-usage-summary.csv    (mÃ©dia de CPU% e memÃ³ria% por container por cenÃ¡rio)
- docker-stats.txt              (snapshot final dos containers)

Use estes arquivos para comparar latÃªncia mÃ©dia, p50, p95, desvio padrÃ£o, throughput efetivo, tempo de resposta de consultas e consumo de recursos entre os cenÃ¡rios.
