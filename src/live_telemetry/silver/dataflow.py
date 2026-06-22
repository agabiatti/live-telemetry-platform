"""Dataflow Bytewax do Silver (speed layer, always-on).

Pipeline:
  kafka(player) → decode Avro → key_on(session_id) → dedupe(event_id) →
  fold_window(1min, event-time, EventClock) → finalize QoE → enrich SCTE → Delta append

Garantias: at-least-once + append-only (idempotência por dedup no read da chave natural
session_id|window_start). Late events (após system_wait) caem em wout.late (DQ runtime).
Ver DESIGN §5. NB: append (não MERGE per-batch) porque o MERGE relê o _delta_log a cada
batch → custo de commit O(n²) que travava o Silver a partir de ~30min de evento.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import pyarrow as pa
import bytewax.operators as op
import bytewax.connectors.kafka.operators as kop
from bytewax.dataflow import Dataflow
from bytewax.operators.windowing import EventClock, TumblingWindower, fold_window
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from live_telemetry.common.config import Config
from live_telemetry.common.logging import get_logger
from live_telemetry.common.metrics import build_acc, finalize, fold_event, merge_acc
from live_telemetry.silver.alerting import BurnRateConfig
from live_telemetry.silver.alerting import step as burn_step
from live_telemetry.silver.dedupe import dedupe_step
from live_telemetry.silver.markers import REGISTRY, start_marker_consumer
from live_telemetry.silver.sink import DeltaAppendSink

log = get_logger(__name__)

# Âncora das janelas tumbling. Deve casar com o cálculo de window_start a partir do id.
ALIGN_TO = datetime(2026, 1, 1, tzinfo=timezone.utc)
DEVICE_SKEW_S = 5


def _parse(iso: str) -> datetime:
    return datetime.fromisoformat(iso.replace("Z", "+00:00"))


def build_flow(cfg: Config) -> Dataflow:
    start_marker_consumer(cfg)

    sr = SchemaRegistryClient({"url": cfg.kafka.schema_registry_url})
    deserialize = AvroDeserializer(sr)
    window_len = cfg.streaming.window_size_s
    silver_path = os.path.join(cfg.paths.silver, "session_window_qoe")

    def decode(msg: Any) -> Optional[dict[str, Any]]:
        try:
            rec = deserialize(msg.value, SerializationContext(msg.topic, MessageField.VALUE))
        except Exception as exc:  # noqa: BLE001 — boundary: mensagem inválida não derruba o stream
            log.error("decode_failed", error=str(exc))
            return None
        if rec is None:
            return None
        rec["event_time"] = _parse(rec["timestamp"])
        device = rec.get("device") or {}
        geo = rec.get("geo") or {}
        rec["device_type"] = device.get("type")
        rec["geo_region"] = geo.get("region")
        return rec

    def finalize_value(window_value: tuple[int, Any]) -> dict[str, Any]:
        window_id, acc = window_value
        row = finalize(acc)
        ws = ALIGN_TO + timedelta(seconds=window_id * window_len)
        row["window_start"] = ws
        row["window_end"] = ws + timedelta(seconds=window_len)
        # Partição do Delta = bucket de 10 min do evento. Restringe o MERGE do sink à partição
        # da janela → upsert não varre a tabela inteira (evita O(n²)). Bucket de 10 min (não a
        # hora) mantém cada partição pequena: ~poucos mil rows → merge rápido mesmo no burst.
        # Separador '-' (sem ':') porque ':' no valor da partição vira %3A inconsistente entre
        # os paths create/merge do deltalake e gera diretório duplicado pra mesma partição.
        row["window_bucket"] = ws.strftime("%Y-%m-%dT%H-") + f"{(ws.minute // 10) * 10:02d}"
        return row

    def enrich(row: dict[str, Any]) -> dict[str, Any]:
        marker = REGISTRY.active(row["window_start"], skew_s=DEVICE_SKEW_S)
        row["marker_id"] = marker["marker_id"] if marker else None
        row["break_type"] = marker["break_type"] if marker else None
        row["scte_event_id"] = marker["event_id_scte"] if marker else None
        row["processed_at"] = datetime.now(timezone.utc)
        return row

    flow = Dataflow("silver")
    kin = kop.input(
        "kafka_player",
        flow,
        brokers=[cfg.kafka.bootstrap],
        topics=[cfg.kafka.topic_player_events],
        add_config={"group.id": "silver-player"},
    )
    decoded = op.filter_map("decode", kin.oks, decode)
    keyed = op.key_on("key_session", decoded, lambda e: e["session_id"])
    deduped = op.stateful_map(
        "dedupe", keyed, lambda s, v: dedupe_step(s, v, cfg.streaming.dedupe_horizon_s)
    )
    fresh = op.filter_value("drop_dups", deduped, lambda pair: not pair[1])
    events = op.map_value("unwrap", fresh, lambda pair: pair[0])

    clock = EventClock(
        ts_getter=lambda e: e["event_time"],
        wait_for_system_duration=timedelta(seconds=cfg.streaming.system_wait_s),
    )
    windower = TumblingWindower(length=timedelta(seconds=window_len), align_to=ALIGN_TO)
    wout = fold_window("qoe_window", events, clock, windower, build_acc, fold_event, merge_acc)

    rows = op.map_value("finalize", wout.down, finalize_value)
    enriched = op.map_value("enrich", rows, enrich)
    plain = op.map("strip_key", enriched, lambda kr: kr[1])
    # Hints p/ colunas que podem vir all-None num batch (1ª janela antes de qualquer marker
    # SCTE chegar): sem isso o pyarrow infere tipo `null` e o Delta rejeita (SchemaMismatchError).
    qoe_hints = {"marker_id": pa.string(), "break_type": pa.string(), "scte_event_id": pa.int64()}
    op.output(
        "delta_sink",
        plain,
        DeltaAppendSink(silver_path, qoe_hints, partition_by=["window_bucket"]),
    )

    # --- Alerta multi-burn-rate por CDN (DQ runtime) ---
    # Reusa as rows QoE: re-keia por CDN; o detector acumula (events, error_count) por minuto
    # incrementalmente conforme as janelas de sessão chegam. Emite só transições FIRING/RESOLVED.
    alert_cfg = BurnRateConfig()
    alerts_path = os.path.join(cfg.paths.silver, "cdn_alerts")

    def burn(state: Any, row: dict[str, Any]) -> tuple[Any, Optional[dict[str, Any]]]:
        rebuffer_ms = row.get("rebuffer_ms") or 0
        watch_ms = row.get("watch_ms") or 0
        # fração ruim = rebuffering ratio = rebuffer_ms / (rebuffer_ms + watch_ms)
        return burn_step(
            state, row.get("cdn") or "unknown", row["window_start"],
            rebuffer_ms + watch_ms, rebuffer_ms, row["events"], alert_cfg,
        )

    cdn_keyed = op.key_on("alert_key", plain, lambda row: row.get("cdn") or "unknown")
    transitions = op.stateful_map("burn_rate", cdn_keyed, burn)
    fired = op.filter_value("fired", transitions, lambda tr: tr is not None)
    alert_rows = op.map(
        "alert_row", fired,
        lambda kr: {**kr[1], "processed_at": datetime.now(timezone.utc)},
    )
    op.output("alert_sink", alert_rows, DeltaAppendSink(alerts_path))
    op.inspect_debug("alert_log", alert_rows,
                     lambda step_id, item, epoch, worker: log.warning("cdn_alert", **item))

    # DQ runtime: late events descartados pela janela.
    op.inspect_debug("late_events", wout.late,
                     lambda step_id, item, epoch, worker: log.warning("late_dropped", item=str(item)[:120]))

    return flow
