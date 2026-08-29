# Validacao final executavel — 29 de agosto de 2026

Este registro documenta a validacao operacional da versao final do prototipo em uma
stack Docker recriada a partir de volumes vazios. Os resultados abaixo comprovam o
funcionamento observado neste ambiente local; nao demonstram alta disponibilidade nem
generalizam desempenho para outros hardwares.

## Ambiente e preparacao

- Docker Engine: 28.0.4.
- Volumes removidos antes da validacao: `pipeline-kafka-data`,
  `pipeline-timescale-data`, `pipeline-grafana-data` e
  `pipeline-flink-checkpoints`.
- As CAGGs foram criadas novamente pelos scripts de inicializacao, incluindo medias de
  preco e latencia ponderadas por `event_count` na agregacao de 15 minutos.

## Smoke test ponta a ponta

O script `scripts/Smoke-Test.ps1` publicou um evento valido e um invalido em uma stack
limpa. Resultado observado:

- um evento valido persistido no TimescaleDB;
- zero persistencias do evento invalido;
- evento invalido encontrado no topico `financial-events-invalid`;
- resultado final: `SMOKE PASS`.

Durante a validacao, o consumidor do smoke foi tornado deterministico: ele enumera as
particoes e le cada uma por offset direto, sem depender do tempo de rebalanceamento de
um grupo Kafka.

## Coleta de recursos

O coletor foi executado por 15 segundos, com intervalo de dois segundos:

- status final: `success`;
- exit code: 0;
- 16 amostras validas de `docker stats`;
- cabecalho, timestamps, container, CPU e memoria validados pelo encerrador;
- zero metricas complementares no endpoint de job Flink.

O ultimo item permanece como limitacao explicita: as amostras de CPU e memoria dos
containers foram coletadas, mas os IDs JVM consultados nao foram expostos pelo endpoint
de metricas do job nesta configuracao.

## Benchmark equivalente raw versus CAGG

Foram inseridos 100.000 eventos sinteticos descartaveis em buckets historicos completos.
O benchmark usou duas execucoes de aquecimento e cinco repeticoes medidas por caminho,
alternando a ordem das consultas.

| caminho | repeticoes | media (ms) | minimo (ms) | maximo (ms) | linhas |
|---|---:|---:|---:|---:|---:|
| raw | 5 | 400,154 | 384,420 | 434,279 | 32 |
| CAGG | 5 | 335,729 | 297,680 | 390,307 | 32 |

Os dois caminhos produziram as mesmas 32 linhas e o mesmo SHA-256:
`dd604d7759ccc36f9db67250bbb45865291d0730901db78d10dce25cb626a5fb`.

Essa observacao valida a equivalencia das consultas e a execucao do protocolo. A
diferenca temporal observada e exploratoria: cinco repeticoes em um unico ambiente nao
sustentam uma conclusao causal ou um fator de speedup generalizavel.

## Recuperacao por checkpoint

Um produtor executou por 40 segundos a 100 eventos por segundo. Depois de um checkpoint
completo, o container TaskManager foi reiniciado.

- job Flink: `cc3203f25c54eb082acfb754e548ffec`;
- checkpoint anterior ao reinicio: 18;
- checkpoint restaurado: 18;
- tentativas do produtor: 4.000;
- confirmacoes Kafka: 4.000;
- falhas de entrega: 0;
- eventos persistidos no TimescaleDB: 4.000;
- exit code do produtor: 0;
- resultado: `PASS`.

O teste comprova recuperacao observada diante do reinicio do TaskManager no ambiente
local. Ele nao testa perda do JobManager, alta disponibilidade do cluster, falha do
broker Kafka ou indisponibilidade do TimescaleDB.

## Correcoes motivadas pela execucao real

1. Captura de stderr de comandos Docker/psql passou a decidir sucesso pelo exit code,
   compativel com Windows PowerShell 5.1.
2. Um servico de inicializacao passou a preparar o volume de checkpoints para o usuario
   `flink` antes de iniciar JobManager e TaskManager.
3. O coletor passou a gravar status atomico em JSON, evitando dependencia de
   `Process.ExitCode` em processos recuperados por `Get-Process`.
4. O SHA-256 do benchmark passou a usar a API compativel com .NET antigo.
5. `elapsed_ms` passou a ser exportado com cultura invariavel e ponto decimal.
