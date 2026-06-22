"""Registro de markers SCTE-35 em memória, alimentado por um consumer em background.

Enriquecimento temporal do Silver: cada janela de sessão é associada ao ad break vigente
(commercial/blackout) cujo intervalo [wallclock, wallclock+duration] cobre a janela, com
tolerância ±skew. Markers são poucos (~10) e chegam ao longo do replay; o consumer roda
numa thread daemon e atualiza o registro thread-safe.

Limitação consciente: enriquecimento é eventualmente-consistente (a janela enxerga o marker
quando ele já chegou). Em produção usaria join/broadcast nativo ou CDC. A verdade definitiva
da associação ad↔sessão é o join batch do Gold.
"""

from __future__ import annotations

import threading
from datetime import datetime, timedelta
from typing import Any, Optional

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from live_telemetry.common.config import Config
from live_telemetry.common.logging import get_logger

log = get_logger(__name__)

AD_BREAK_TYPES = ("commercial", "blackout")


def _parse(iso: str) -> datetime:
    return datetime.fromisoformat(iso.replace("Z", "+00:00"))


class MarkerRegistry:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._markers: list[tuple[datetime, datetime, dict[str, Any]]] = []

    def add(self, marker: dict[str, Any]) -> None:
        start = _parse(marker["wallclock"])
        end = start + timedelta(seconds=marker.get("duration_s", 0))
        with self._lock:
            self._markers.append((start, end, marker))

    def active(self, t: datetime, skew_s: int = 5) -> Optional[dict[str, Any]]:
        slack = timedelta(seconds=skew_s)
        with self._lock:
            for start, end, marker in self._markers:
                if marker["break_type"] in AD_BREAK_TYPES and (start - slack) <= t <= (end + slack):
                    return marker
        return None


REGISTRY = MarkerRegistry()


def start_marker_consumer(cfg: Config) -> threading.Thread:
    """Inicia thread daemon que consome scte35 e alimenta o REGISTRY."""

    def _run() -> None:
        sr = SchemaRegistryClient({"url": cfg.kafka.schema_registry_url})
        deserialize = AvroDeserializer(sr)
        consumer = Consumer({
            "bootstrap.servers": cfg.kafka.bootstrap,
            "group.id": "silver-scte-markers",
            "auto.offset.reset": "earliest",
        })
        consumer.subscribe([cfg.kafka.topic_scte35])
        log.info("marker_consumer_started", topic=cfg.kafka.topic_scte35)
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            rec = deserialize(msg.value(), SerializationContext(cfg.kafka.topic_scte35, MessageField.VALUE))
            REGISTRY.add(rec)
            log.info("marker_loaded", marker_id=rec.get("marker_id"), break_type=rec.get("break_type"))

    thread = threading.Thread(target=_run, name="scte-marker-consumer", daemon=True)
    thread.start()
    return thread
