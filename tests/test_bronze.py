"""Testes das transformações Bronze (puras, sem broker)."""

from __future__ import annotations

from datetime import datetime, timezone

from live_telemetry.bronze.transform import flatten_content, flatten_player, flatten_scte

KMETA = {"partition": 2, "offset": 42, "ingested_at": datetime(2026, 5, 20, 22, 14, 34, tzinfo=timezone.utc)}

PLAYER_V1 = {
    "event_id": "e1", "session_id": "s1", "user_id": "u1",
    "timestamp": "2026-05-20T22:14:33.812Z", "event_type": "heartbeat",
    "content_id": "live-brasileirao-final-2026", "is_live": True,
    "device": {"type": "smart_tv", "model": "LG", "os": "webOS", "app_version": "1.42.0"},
    "geo": {"region": "SE", "state": "RJ", "city": "Rio", "isp": "isp-vivo"},
    "cdn": "cdn-a", "bitrate_kbps": 5800, "buffer_length_ms": 12000,
    "playhead_position_s": 4823, "error_code": None,
}


def test_flatten_player_v1_has_null_v2_fields() -> None:
    out = flatten_player(PLAYER_V1, KMETA)
    assert out["device_type"] == "smart_tv"
    assert out["geo_region"] == "SE"
    assert out["schema_version"] is None  # Bronze guarda o cru: v1 não tem o campo
    assert out["network_type"] is None
    assert out["event_hour"] == "2026-05-20T22"
    assert out["_kafka_offset"] == 42
    assert out["event_time"].year == 2026


def test_flatten_player_v2_preserves_network_type() -> None:
    v2 = {**PLAYER_V1, "schema_version": "v2", "network_type": "cellular"}
    out = flatten_player(v2, KMETA)
    assert out["schema_version"] == "v2"
    assert out["network_type"] == "cellular"


def test_flatten_scte() -> None:
    rec = {
        "marker_id": "m1", "channel": "ch", "splice_command": "splice_insert",
        "event_id_scte": 1003, "out_of_network": True, "pts_time": 459000000,
        "wallclock": "2026-05-20T22:14:30.000Z", "duration_s": 60, "break_type": "commercial",
    }
    out = flatten_scte(rec, KMETA)
    assert out["break_type"] == "commercial"
    assert out["wallclock"].hour == 22
    assert out["_kafka_partition"] == 2


def test_flatten_content_adds_metadata() -> None:
    rec = {"content_id": "c1", "title": "x", "genre": "sports"}
    out = flatten_content(rec, KMETA)
    assert out["content_id"] == "c1"
    assert out["_ingested_at"] == KMETA["ingested_at"]
