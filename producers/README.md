# Producers de Eventos Financeiros

Este componente implementa a geração sintética de eventos financeiros usados para alimentar a pipeline.

O serviço Docker Compose continua se chamando `producer`, para permitir comandos como `--scale producer=3`, mas o código-fonte fica no diretório `producers/` porque agora existem diferentes perfis de geração.

## Contrato publicado

Cada mensagem enviada ao Kafka é publicada no tópico definido por `KAFKA_TOPIC_RAW`, usando `symbol` como chave de particionamento.

```json
{
  "event_id": "uuid",
  "producer_id": "producer-1",
  "symbol": "PETR4",
  "price": 38.42,
  "quantity": 100,
  "event_time": "2026-04-28T14:00:00.123Z",
  "producer_time": "2026-04-28T14:00:00.130Z",
  "source": "synthetic",
  "scenario": "low",
  "sequence": 1
}
```

## Tipos de producer

| Tipo | Variável | Comportamento |
| --- | --- | --- |
| Aleatório | `PRODUCER_TYPE=random` | Gera preços com pequenas variações independentes em torno do preço base. É o modo padrão para validar a pipeline. |
| Tendência | `PRODUCER_TYPE=trend` | Mantém estado por ativo e simula caminhada temporal com drift. É útil para observar séries com direção de curto prazo. |
| Rajada | `PRODUCER_TYPE=burst` | Gera períodos de variação e volume maiores. É útil para testar latência, throughput e pressão sobre Kafka/Flink/TimescaleDB. |

## Variáveis de ambiente

| Variável | Padrão | Descrição |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Endereço do Kafka dentro da rede Docker |
| `KAFKA_TOPIC_RAW` | `financial-events-raw` | Tópico bruto de destino |
| `KAFKA_TOPIC_EVENTS` | `financial-events` | Fallback legado, caso `KAFKA_TOPIC_RAW` não seja definido |
| `PRODUCER_ID` | hostname do contêiner | Identificador do produtor |
| `PRODUCER_TYPE` | `random` | Tipo de geração: `random`, `trend` ou `burst` |
| `PRODUCER_SYMBOLS` | lista padrão | Ativos separados por vírgula |
| `PRODUCER_RATE_PER_SECOND` | `10` | Eventos por segundo por instância |
| `PRODUCER_SCENARIO` | `low` | Nome do cenário experimental |
| `PRODUCER_RUN_DURATION_SECONDS` | `0` | Duração da execução; `0` significa contínuo |
| `PRODUCER_RANDOM_SEED` | vazio | Seed opcional para repetibilidade |
| `PRODUCER_SOURCE` | `synthetic` | Origem registrada no evento |
| `PRODUCER_VOLATILITY` | `0.015` | Volatilidade base usada na variação de preços |
| `PRODUCER_TREND_STRENGTH` | `0.002` | Intensidade máxima do drift no modo `trend` |
| `PRODUCER_BURST_PROBABILITY` | `0.05` | Probabilidade de rajada no modo `burst` |

## Execução via Docker Compose

Subir infraestrutura e producer padrão:

```powershell
docker compose --profile app up -d producer
```

Rodar três producers em paralelo:

```powershell
docker compose --profile app up -d --scale producer=3 producer
```

Cada réplica usa o hostname do contêiner como `producer_id` quando `PRODUCER_ID` não é informado.

Executar um producer de tendência por 60 segundos a 100 eventos/s:

```powershell
docker compose run --rm -e PRODUCER_TYPE=trend -e PRODUCER_RATE_PER_SECOND=100 -e PRODUCER_SCENARIO=medium -e PRODUCER_RUN_DURATION_SECONDS=60 producer
```

Executar um producer de rajada:

```powershell
docker compose run --rm -e PRODUCER_TYPE=burst -e PRODUCER_RATE_PER_SECOND=200 -e PRODUCER_SCENARIO=stress -e PRODUCER_RUN_DURATION_SECONDS=60 producer
```

Ver logs:

```powershell
docker compose logs -f producer
```

## Verificação no Kafka

Depois de subir Kafka e producer, é possível inspecionar mensagens com:

```powershell
docker compose exec kafka kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic financial-events-raw --from-beginning --max-messages 5
```

Critério de aceite: o comando acima deve retornar eventos JSON válidos no tópico `financial-events-raw`.
