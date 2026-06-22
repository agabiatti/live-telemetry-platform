# OBSERVABILITY — catálogo de Data Quality checks

Catálogo dos DQ checks da `live-telemetry-platform`, mapeados às **4 dimensões**
exigidas pelo desafio (F5 "Observabilidade de dados"): **freshness, volume,
schema, distribution/quality**. Os targets vivem em [`slos.yaml`](./slos.yaml)
(fonte única); o detector multi-burn-rate (DESIGN §6) reusa o SLO de QoE.

## Honestidade: implementado vs spec

Cada check é marcado com `status:` em `slos.yaml`:

- **`implemented`** — o código emite/executa o check **hoje**, rastreável a arquivo.
- **`spec`** — SLO/target acordado, mas a **automação do check ainda é roadmap**.

A entrega atual roda os `implemented`; os `spec` documentam o contrato-alvo de
qualidade sem fingir que já rodam. F5 exige **≥ 1 check em runtime**: temos
**um runtime de fato** — o detector **multi-burn-rate** — mais o sinal runtime
**`late_dropped`** (hoje emitido como log).

---

## Inventário

### ✅ Implementados (rodam na entrega atual)

| Check | Dimensão | Onde roda | Prova (arquivo) |
|---|---|---|---|
| `multi_burn_rate_cdn` | distribution | **Runtime (Silver/Bytewax)** | `silver/alerting.py` → `cdn_alerts` Delta + log `cdn_alert` |
| `late_dropped` | freshness | **Runtime (Silver)** | `silver/dataflow.py` `inspect_debug(wout.late)` → log `late_dropped` |
| `registry_incompatible_reject` | schema | Gate de contrato (registro/CI) | `contracts/registry.py` `set_backward` (rejeita incompatível) |
| `gold_qoe_not_null/unique` | quality | Pós-batch (`dbt test`) | `dbt/models/marts/schema.yml`: `unique`+`not_null` em `window_key`, `not_null` em `session_id`/`window_start`/`rebuffering_ratio` |
| `gold_ccv_not_null` | quality | Pós-batch (`dbt test`) | `schema.yml`: `not_null` em `ccv` |
| `gold_ad_impact_checks` | schema/distribution | Pós-batch (`dbt test`) | `schema.yml`: `not_null` em `event_id_scte`, `accepted_values` em `break_type` |

→ **6 checks implementados**, cobrindo distribution, freshness, schema e quality.
O único runtime-de-streaming **contínuo** é o `multi_burn_rate_cdn` (atende F5);
`late_dropped` é o 2º sinal runtime.

### 🗺️ Spec / roadmap (SLO escrito, automação pendente)

| Check | Dimensão | Plano | Por que ainda não |
|---|---|---|---|
| `silver_watermark_lag` | freshness | emitir `now - W` do EventClock no Silver | só `watermark_delay_s` existe no config; o lag não é calculado |
| `ingestion_lag` | freshness | medir p95(`ingestion_time - event_time`) no Bronze | carimbo existe; agregação p95 não |
| `gold_freshness` | freshness | `dbt source freshness` em `sources.yml` | bloco `freshness:` não configurado |
| `event_count_vs_baseline` | volume | contagem/janela vs mediana móvel 15m no Silver | não emitido |
| `dedupe_dropped_ratio` | volume | contador de `is_dup` no `dedupe_step` | dedupe roda mas descarta sem contar |
| `schema_v2_share` | schema | % decodificado com schema-id v2 no Bronze | não computado |
| `slo_error_rate_cdn` | distribution | 2º detector burn-rate sobre `error_rate` | `error_rate` é computado em `metrics.finalize`, não gateado |
| `silver_gold_divergence` | distribution | `dbt test` comparando Silver×Gold por chave | cross-check não materializado |

---

## Detalhamento dos implementados

### `multi_burn_rate_cdn` — **RUNTIME (streaming)**
- **Dimensão:** distribution. **Onde:** Silver (Bytewax), event-time, **por CDN ativa**.
- **O que mede:** `burn_rate = fração_ruim / budget`, com `budget = SLO = 0.01`. A
  fração-ruim consumida hoje é **só o `rebuffering_ratio`** (`rebuffer_ms /
  (rebuffer_ms + watch_ms)`); `error_rate` fica como sinal alternativo (roadmap).
  Dois tiers (**fast** 5m/1m, burn ≥ **4**, page; **slow** 30m/5m, burn ≥ **2**,
  ticket), multi-window AND, gate de amostra (`n_min: 50`) e histerese anti-flap
  (fire 2 / clear 3). Números espelham `BurnRateConfig` em `silver/alerting.py`.
- **Dinâmico:** avalia f(cdn) para toda CDN; cdn-b dispara nesta seed por cruzar o
  critério (burst min 60-75: rebuffering ~8% => burn ~8), **não por hardcode**.
- **SLO:** `slo_rebuffering_cdn` ≤ 1%, severity `page`.

### `late_dropped` — **RUNTIME (streaming)**
- **Dimensão:** freshness. **Onde:** Silver, side output `wout.late` da janela.
- **O que mede:** eventos além do `system_wait` do EventClock → log `late_dropped`.
  Não é perda: o **Gold reconcilia** o late no período fechado (DESIGN §7). Pico
  sustentado = backlog drenando pós broker-down (RUNBOOK incidente A).
- **SLO:** `late_dropped` — fração ≤ 2% em regime normal, severity `warn`.

### `registry_incompatible_reject` — gate de contrato
- **Dimensão:** schema. **Onde:** Schema Registry, compat **BACKWARD**.
- **O que faz:** `registry.set_backward` fixa BACKWARD por subject; schema novo
  **sem `default`** é **rejeitado** no `register` (`is_compatible:false`). É um
  **gate ativo** (proteção real), não uma taxa amostrada. Drift barrado antes do
  fio. A verdade da versão é o **schema-id no fio**, não o campo `schema_version`.
  Resposta a incidente: [`RUNBOOK.md`](../RUNBOOK.md) incidente B.

### dbt tests no Gold — pós-batch (a verdade reconciliada)
- **Dimensão:** quality + schema. **Onde:** `dbt test` sobre os marts Gold.
- **O que cobre:** unicidade da chave natural (`window_key`), `not_null` nas
  métricas/chaves críticas (`session_id`, `window_start`, `rebuffering_ratio`,
  `ccv`, `event_id_scte`) e domínio fechado (`break_type ∈ {commercial, blackout}`).
  São o DQ pós-batch real da entrega (8/8 tests passam — RESUME #7).

---

## Mapa dimensão → check (implementado | spec)

- **freshness:** `late_dropped` ✅ | `silver_watermark_lag`, `ingestion_lag`, `gold_freshness` 🗺️
- **volume:** — | `event_count_vs_baseline`, `dedupe_dropped_ratio` 🗺️
- **schema:** `registry_incompatible_reject` ✅, `accepted_values(break_type)` ✅ | `schema_v2_share` 🗺️
- **distribution/quality:** `multi_burn_rate_cdn` ✅, dbt `not_null`/`unique` ✅ | `slo_error_rate_cdn`, `silver_gold_divergence` 🗺️
