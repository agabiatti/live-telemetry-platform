# Live Telemetry Platform

Plataforma **Lambda** de telemetria de audiência para transmissões ao vivo.
Speed layer (Silver, near real-time) + batch layer (Bronze→Gold, verdade auditável),
servindo três consumidores com SLAs distintos: comercial (CCV near real-time), 
produto (QoE/CDN ao vivo) e financeiro (reconciliação idempotente).

**Decisões e trade-offs:** [`DESIGN.md`](DESIGN.md).

## Quickstart

Para preservar o teste, o gerador e o desafio não estão no repositório.
Para rodar o projeto, é necessário ter o `live-telemetry-platform` e o `desafio-data-engineer`
no mesmo diretório.

Exemplo:

```
challenge
├── live-telemetry-platform/
└── desafio-data-engineer/
```

Todo o projeto é dockerizado e não precisa instalar nada localmente para rodar.

## Como executar 

```bash
cp env.example .env          # ajuste se quiser (REPLAY_SPEED etc.)
make generate                # gera dataset sintético em ./data/raw (container)
make demo                    # sobe tudo + roda o pipeline ponta a ponta
# dashboard: http://localhost:8501   |   console Redpanda: http://localhost:8080
```

`make help` lista todos os targets. Principais:

| Target | O que faz |
|---|---|
| `make demo` | pipeline ponta a ponta (up + producer + bronze + gold) |
| `make generate` | gera o dataset (container, `N_SESSIONS=2000`) |
| `make up` | infra + serviços always-on (redpanda, console, silver, streamlit) |
| `make producer / bronze / gold` | etapas individuais |
| `make test` | suíte unit (monta o repo no container) |
| `make smoke` | **smoke e2e** isolado em `data/smoke` (gera→broker→bronze→gold→asserts) |
| `make clean` | derruba + remove volumes + limpa o lake local |

## Como executar passo a passo e acompanhar a execução

```bash
make generate N_SESSIONS=2000               
make up            # Sobe todos os serviços.
make producer      # Publica as mensagens no RedPanda

# Aqui já é possível acompanhar o dashboard e os dados do streaming → http://localhost:8501

make bronze        # Bronze Delta
make gold          # Gold (dbt → Parquet)

# Caso rode para um número maior de sessões, é necessário rodar o compact para diminuir o
# número de arquivos e melhorar a performance de leitura da silver.

make compact
```

**Persiste** em `./data/` (bind-mount): Bronze/Silver/Gold + `cdn_alerts`. **Não persiste:**
tópicos do Redpanda e subjects do Schema Registry (efêmeros, de propósito — o producer
re-registra e republica).

## Testes

```bash
make test     # 25 unit (contratos, bronze, silver, alerting, sink, schema-evolution) + 4 smoke skipped
make smoke    # roda a cadeia real via broker e os 4 asserts e2e
```

Cobertura: transformações Silver, contratos (v1↔v2, default-fill), bronze flatten, alerta
multi-burn-rate, sink Delta (regressão de coluna all-None), propagação de schema evolution
(roda o **SQL real do dbt** em DuckDB), e smoke e2e da cadeia.

## Stack

| Camada | Escolha |
|---|---|
| Broker | **Redpanda** (Kafka API, sem ZK) | replay/recovery reais, leve |
| Contratos | **Avro + Schema Registry** (BACKWARD) | reader/writer resolution p/ schema evolution |
| Bronze | **Delta** (delta-rs, sem Spark) | overwrite atômico + log p/ auditoria — |
| Silver | **Bytewax** (Python, at-least-once + sink idempotente) | event-time/window nativo, Python-native |
| Gold | **dbt + DuckDB** (Parquet + manifest) | lineage/tests/docs como produto |
| Serving | **Streamlit** sobre DuckDB | 4 widgets com menos código |
| Logs | structlog (JSON estruturado) | — |

Topologia **Lambda**: batch é a verdade, speed é aproximação realtime. 

Detalhes em [`DESIGN.md`](DESIGN.md).

## As 5 anomalias injetadas (e como são tratadas)

| Anomalia | Tratamento |
|---|---|
| Out-of-order (≤ 15 s) | watermark `delay ≈ 25 s` (event-time) |
| Duplicatas (~1 %) | dedupe set por `event_id` (Silver, bounded) + dedupe global (Gold) |
| Schema v2 (~0.5 %, `network_type`) | Avro BACKWARD + default-fill; cadeia em `docs/schema-evolution.md` |
| Burst `cdn-b` (min 60-75) | alerta multi-burn-rate dinâmico por CDN (rebuffering) |
| Clock skew (±5 s) | QoE skew-invariante; tolerância na janela cross-device |

## Tempo Gasto

Acredito ter ultrapassado um pouco o tempo proposto para o teste (6 a 10 horas de dedicação).
Durante a semana consegui dedicar no máximo 4 horas para estudar o desafio e esboçar uma solução.
Então concentrei o trabalho de construção no fim de semana para finalizar o pipeline no prazo.