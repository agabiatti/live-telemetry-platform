"""Job Bronze (batch, one-shot) — drena os tópicos → tabelas Delta imutáveis.

Estratégia: consome cada tópico do início até o fim atual (drain), deserializa Avro
(schema-id no fio), achata e escreve Delta em modo **overwrite** (snapshot determinístico
do tópico). Re-rodar com o mesmo tópico produz Bronze idêntica → idempotência.

Imutabilidade é política: nunca mutamos registros; um re-drain substitui o snapshot inteiro.

Uso: python -m live_telemetry.bronze.job
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import pyarrow as pa
from confluent_kafka import Consumer, Message
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from deltalake import write_deltalake

from live_telemetry.common.config import Config, load_config
from live_telemetry.common.logging import configure_logging, get_logger
from live_telemetry.bronze.transform import flatten_content, flatten_player, flatten_scte

log = get_logger(__name__)

IDLE_POLLS_TO_STOP = 8


def _kafka_ingested_at(msg: Message) -> datetime:
    _, ms = msg.timestamp()
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def drain(cfg: Config, topic: str) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    """Consome o tópico do início ao fim atual. Retorna (record, kafka_meta)."""
    sr = SchemaRegistryClient({"url": cfg.kafka.schema_registry_url})
    deserialize = AvroDeserializer(sr)
    consumer = Consumer({
        "bootstrap.servers": cfg.kafka.bootstrap,
        "group.id": f"bronze-{topic}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic])

    rows: list[tuple[dict[str, Any], dict[str, Any]]] = []
    idle = 0
    try:
        while idle < IDLE_POLLS_TO_STOP:
            msg = consumer.poll(1.0)
            if msg is None:
                idle += 1
                continue
            if msg.error():
                continue
            idle = 0
            rec = deserialize(msg.value(), SerializationContext(topic, MessageField.VALUE))
            kmeta = {
                "partition": msg.partition(),
                "offset": msg.offset(),
                "ingested_at": _kafka_ingested_at(msg),
            }
            rows.append((rec, kmeta))
    finally:
        consumer.close()
    return rows


def _write(path: str, records: list[dict[str, Any]], partition_by: list[str] | None) -> int:
    if not records:
        log.warning("bronze_empty", path=path)
        return 0
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(records)
    write_deltalake(path, table, mode="overwrite", partition_by=partition_by or [])
    return len(records)


def _land(
    cfg: Config,
    topic: str,
    table_name: str,
    flatten: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]],
    partition_by: list[str] | None = None,
) -> int:
    drained = drain(cfg, topic)
    records = [flatten(rec, kmeta) for rec, kmeta in drained]
    path = str(Path(cfg.paths.bronze) / table_name)
    n = _write(path, records, partition_by)
    log.info("bronze_landed", table=table_name, rows=n, path=path)
    return n


def main() -> int:
    configure_logging()
    cfg = load_config()
    log.info("bronze_start", bootstrap=cfg.kafka.bootstrap, path=cfg.paths.bronze)

    _land(cfg, cfg.kafka.topic_player_events, "player_events", flatten_player, partition_by=["event_hour"])
    _land(cfg, cfg.kafka.topic_scte35, "scte35_markers", flatten_scte)
    _land(cfg, cfg.kafka.topic_content, "content_metadata", flatten_content)

    log.info("bronze_done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
