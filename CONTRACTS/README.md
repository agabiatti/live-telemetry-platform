# CONTRACTS — schemas Avro versionados

Contratos dos 3 streams principais. A versão **canônica** de um payload é o **schema-id no
fio** (Confluent wire-format), resolvido contra o Schema Registry — não um campo no corpo.

| Stream | Tópico | Subject (TopicNameStrategy) | Schemas |
|---|---|---|---|
| Player events | `player_events` | `player_events-value` | `player_events.v1`, `player_events.v2` |
| SCTE-35 | `scte35_markers` | `scte35_markers-value` | `scte35_markers.v1` |
| Content | `content_metadata` | `content_metadata-value` | `content_metadata.v1` |

`ad_decisions` é bônus (uso opcional na reconciliação Gold) e não tem contrato versionado aqui.

## Política de compatibilidade: **BACKWARD**

**Definição:** um consumidor com o schema **novo** consegue ler dados escritos com o schema
**antigo**. É evolução *consumer-first* (consumidores atualizam antes dos producers).

**Por que BACKWARD (e não FORWARD/FULL):**
- Queremos atualizar consumidores e **reprocessar histórico** com o schema novo (ex.: Gold lê
  Bronze antigo já com `network_type`). BACKWARD garante isso.
- BACKWARD **força que todo campo novo tenha `default`** — exatamente o que torna a evolução
  segura. `network_type` tem `default: "unknown"`.
- FULL seria mais restritivo (proíbe também remoções com default) sem ganho neste cenário.

**Nota sobre consumidores antigos lendo dados novos:** embora o *modo* seja BACKWARD, como
`network_type` foi adicionado **com default**, a resolução Avro também deixa um reader **v1**
ler um payload **v2** (ignora o campo desconhecido). Então nem consumidor antigo nem novo
quebram — demonstrado nos testes (`tests/test_contracts.py`).

## Evolução v1 → v2 (campo `network_type`) — F6

- **v1:** schema base.
- **v2:** adiciona `network_type` (enum `wifi|cellular|ethernet|unknown`, default `unknown`) e
  `schema_version` (string, default `v1`, redundante e só para debug).
- O gerador emite ~0,5% das mensagens em v2 (com `network_type`). O **producer escolhe o schema
  por mensagem** (tem `network_type` → v2; senão → v1); ambos coexistem no mesmo subject como
  versões 1 e 2. O schema-id no fio identifica qual foi usado.

| Direção | Mecanismo | Garantia |
|---|---|---|
| reader **v2** lê dado **v1** | default fill | `network_type = "unknown"` (BACKWARD) |
| reader **v1** lê dado **v2** | ignora campo desconhecido | não quebra (resolução Avro) |

## Decisões de modelagem (trade-offs)

- **Timestamps = string ISO-8601** no contrato. Fiel ao gerador (o clock skew vive no próprio
  ISO); o parse para tipo temporal acontece downstream. Trade-off: simplicidade/fidelidade vs
  logical-type tipado (`timestamp-millis`). Para produção, migrar para logical types reduz bytes
  e ambiguidade — listado no roadmap.
- **Enum onde o domínio é fechado e estável** (`network_type`, `splice_command`, `break_type`);
  **string onde há extensibilidade** (`event_type` com `seek` reservado; `genre`).
- **`classification` = string** (símbolos `"10"`, `"12"` são inválidos como enum Avro — devem
  casar `[A-Za-z_][A-Za-z0-9_]*`).
- **`pts_time` = long** (ticks 90 kHz podem exceder int32).

## Como registrar

```bash
python -m live_telemetry.contracts.register   # idempotente: v1 → set BACKWARD → v2
```

CI (roadmap): um check de compat bloqueia PR que registre schema incompatível.
