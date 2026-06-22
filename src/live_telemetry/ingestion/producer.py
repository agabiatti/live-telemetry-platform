"""Producer / ingestão — replay do dataset JSONL para o Redpanda.

Modelo:
- `content_metadata` é dimensão estática → publicada upfront (antes do replay).
- `player_events` + `scte35_markers` entram numa timeline única ordenada por event-time
  e são reproduzidos com aceleração `REPLAY_SPEED` (default 60), preservando o espaçamento
  temporal entre eventos. Markers SCTE disparam nos minutos certos → join temporal realista.

Garantias / decisões:
- Schema por mensagem: player com `network_type` → v2, senão v1 (coexistência no subject).
- Particionamento de `player_events` por `session_id` → ordering por sessão.
- Ingestion-time = timestamp nativo do registro Kafka (não polui o contrato).
- Pacing por wall-clock alvo (auto-corrige drift), não sleep por gap.

Uso: python -m live_telemetry.ingestion.producer
"""

from __future__ import annotations

import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

from live_telemetry.common.config import Config, load_config
from live_telemetry.common.logging import configure_logging, get_logger
from live_telemetry.contracts.register import bootstrap
from live_telemetry.contracts.schemas import load_schema_str
from live_telemetry.contracts.serde import make_serializer

log = get_logger(__name__)

PLAYER_PARTITIONS = 6  # múltiplas partições para o particionamento por session_id ter efeito


# --------------------------------------------------------------------------- IO

def read_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def parse_event_time(iso: str) -> datetime:
    return datetime.fromisoformat(iso.replace("Z", "+00:00"))


# --------------------------------------------------------------------- broker

def ensure_topics(cfg: Config) -> None:
    admin = AdminClient({"bootstrap.servers": cfg.kafka.bootstrap})
    topics = [
        NewTopic(cfg.kafka.topic_player_events, num_partitions=PLAYER_PARTITIONS, replication_factor=1),
        NewTopic(cfg.kafka.topic_scte35, num_partitions=1, replication_factor=1),
        NewTopic(cfg.kafka.topic_content, num_partitions=1, replication_factor=1),
    ]
    for topic, future in admin.create_topics(topics).items():
        try:
            future.result()
            log.info("topic_created", topic=topic)
        except Exception as exc:  # noqa: BLE001 — "já existe" é benigno
            log.info("topic_exists_or_skip", topic=topic, detail=str(exc).split(":")[-1].strip())


def _delivery_report(err, msg) -> None:
    if err is not None:
        log.error("delivery_failed", topic=msg.topic(), error=str(err))


# ----------------------------------------------------------------- timeline

class StreamPublisher:
    """Encapsula serializers e o produce por stream."""

    def __init__(self, cfg: Config) -> None:
        sr = SchemaRegistryClient({"url": cfg.kafka.schema_registry_url})
        self._cfg = cfg
        self._producer = Producer({"bootstrap.servers": cfg.kafka.bootstrap, "linger.ms": 50})
        self._key_ser = StringSerializer("utf_8")
        self._ser_player_v1: AvroSerializer = make_serializer(sr, load_schema_str("player_events.v1"))
        self._ser_player_v2: AvroSerializer = make_serializer(sr, load_schema_str("player_events.v2"))
        self._ser_scte: AvroSerializer = make_serializer(sr, load_schema_str("scte35_markers.v1"))
        self._ser_content: AvroSerializer = make_serializer(sr, load_schema_str("content_metadata.v1"))

    def _produce(self, topic: str, key: str, value: bytes) -> None:
        self._producer.produce(
            topic,
            key=self._key_ser(key, SerializationContext(topic, MessageField.KEY)),
            value=value,
            on_delivery=_delivery_report,
        )
        self._producer.poll(0)  # serve callbacks sem bloquear

    def publish_player(self, rec: dict[str, Any]) -> None:
        topic = self._cfg.kafka.topic_player_events
        ser = self._ser_player_v2 if "network_type" in rec else self._ser_player_v1
        value = ser(rec, SerializationContext(topic, MessageField.VALUE))
        self._produce(topic, rec["session_id"], value)

    def publish_scte(self, rec: dict[str, Any]) -> None:
        topic = self._cfg.kafka.topic_scte35
        value = self._ser_scte(rec, SerializationContext(topic, MessageField.VALUE))
        self._produce(topic, rec["channel"], value)

    def publish_content(self, rec: dict[str, Any]) -> None:
        topic = self._cfg.kafka.topic_content
        value = self._ser_content(rec, SerializationContext(topic, MessageField.VALUE))
        self._produce(topic, rec["content_id"], value)

    def flush(self) -> None:
        self._producer.flush()


def build_timeline(cfg: Config) -> list[tuple[datetime, str, dict[str, Any]]]:
    """Player + SCTE numa timeline única ordenada por event-time."""
    raw = Path(cfg.paths.raw)
    timeline: list[tuple[datetime, str, dict[str, Any]]] = []
    for rec in read_jsonl(raw / "player_events.jsonl"):
        timeline.append((parse_event_time(rec["timestamp"]), "player", rec))
    scte_path = raw / "scte35_markers.jsonl"
    if scte_path.exists():
        for rec in read_jsonl(scte_path):
            timeline.append((parse_event_time(rec["wallclock"]), "scte", rec))
    timeline.sort(key=lambda item: item[0])
    return timeline


def replay(cfg: Config, pub: StreamPublisher, timeline: list[tuple[datetime, str, dict[str, Any]]]) -> None:
    if not timeline:
        log.warning("empty_timeline")
        return

    speed = max(1, cfg.replay.speed)
    first_et = timeline[0][0]
    start = time.monotonic()
    published = 0

    for event_time, stream, rec in timeline:
        # Pacing: alvo de wall-clock = início + (offset event-time / speed). Auto-corrige drift.
        target = start + (event_time - first_et).total_seconds() / speed
        delay = target - time.monotonic()
        if delay > 0:
            time.sleep(delay)

        if stream == "player":
            pub.publish_player(rec)
        else:
            pub.publish_scte(rec)

        published += 1
        if published % 50_000 == 0:
            log.info("replay_progress", published=published, total=len(timeline))

    pub.flush()
    elapsed = time.monotonic() - start
    log.info("replay_done", published=published, elapsed_s=round(elapsed, 1), speed=speed)


def main() -> int:
    configure_logging()
    cfg = load_config()

    log.info("ingestion_start", speed=cfg.replay.speed, bootstrap=cfg.kafka.bootstrap)
    bootstrap()           # garante contratos v1→BACKWARD→v2 (idempotente)
    ensure_topics(cfg)

    pub = StreamPublisher(cfg)

    # Dimensão estática upfront
    content_path = Path(cfg.paths.raw) / "content_metadata.jsonl"
    n_content = 0
    if content_path.exists():
        for rec in read_jsonl(content_path):
            pub.publish_content(rec)
            n_content += 1
        pub.flush()
    log.info("content_published", count=n_content)

    timeline = build_timeline(cfg)
    log.info("timeline_built", events=len(timeline))
    replay(cfg, pub, timeline)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
