"""Transformações puras Bronze (testáveis sem broker).

Bronze é a camada imutável e crua: NÃO deduplica, NÃO reordena, NÃO corrige skew.
Mantém duplicatas, out-of-order e schema v2 como vieram — é a fonte replayável.
Só achata structs aninhados (device/geo) e anexa metadados de event-time + ingestão.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any


def parse_event_time(iso: str) -> datetime:
    return datetime.fromisoformat(iso.replace("Z", "+00:00"))


def flatten_player(rec: dict[str, Any], kmeta: dict[str, Any]) -> dict[str, Any]:
    device = rec.get("device") or {}
    geo = rec.get("geo") or {}
    event_time = parse_event_time(rec["timestamp"])
    return {
        "event_id": rec["event_id"],
        "session_id": rec["session_id"],
        "user_id": rec["user_id"],
        "event_time": event_time,
        "event_type": rec["event_type"],
        "content_id": rec["content_id"],
        "is_live": rec["is_live"],
        "device_type": device.get("type"),
        "device_model": device.get("model"),
        "device_os": device.get("os"),
        "app_version": device.get("app_version"),
        "geo_region": geo.get("region"),
        "geo_state": geo.get("state"),
        "geo_city": geo.get("city"),
        "geo_isp": geo.get("isp"),
        "cdn": rec["cdn"],
        "bitrate_kbps": rec["bitrate_kbps"],
        "buffer_length_ms": rec["buffer_length_ms"],
        "playhead_position_s": rec["playhead_position_s"],
        "error_code": rec.get("error_code"),
        # v2 — nulos em mensagens v1 (default não aplicado: Bronze guarda o cru)
        "schema_version": rec.get("schema_version"),
        "network_type": rec.get("network_type"),
        # partição + metadados de ingestão
        "event_hour": event_time.strftime("%Y-%m-%dT%H"),
        "_ingested_at": kmeta["ingested_at"],
        "_kafka_partition": kmeta["partition"],
        "_kafka_offset": kmeta["offset"],
    }


def flatten_scte(rec: dict[str, Any], kmeta: dict[str, Any]) -> dict[str, Any]:
    wallclock = parse_event_time(rec["wallclock"])
    return {
        "marker_id": rec["marker_id"],
        "channel": rec["channel"],
        "splice_command": rec["splice_command"],
        "event_id_scte": rec["event_id_scte"],
        "out_of_network": rec["out_of_network"],
        "pts_time": rec["pts_time"],
        "wallclock": wallclock,
        "duration_s": rec["duration_s"],
        "break_type": rec["break_type"],
        "_ingested_at": kmeta["ingested_at"],
        "_kafka_partition": kmeta["partition"],
        "_kafka_offset": kmeta["offset"],
    }


def flatten_content(rec: dict[str, Any], kmeta: dict[str, Any]) -> dict[str, Any]:
    out = dict(rec)  # catálogo é flat na origem
    out["_ingested_at"] = kmeta["ingested_at"]
    out["_kafka_partition"] = kmeta["partition"]
    out["_kafka_offset"] = kmeta["offset"]
    return out
