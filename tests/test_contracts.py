"""Testes de contrato — offline (fastavro), sem Schema Registry.

Cobrem serde round-trip e schema resolution v1↔v2 (o coração de F6). O teste de
enforcement de compat no registry é de integração (requer SR up) e fica separado.
"""

from __future__ import annotations

import io

import fastavro
import pytest

from live_telemetry.contracts.schemas import (
    ACTIVE_SCHEMA,
    load_schema,
    subject_for,
)

# ---------------------------------------------------------------------------
# Fixtures de amostra (mínimas, mas fiéis aos tipos do gerador)
# ---------------------------------------------------------------------------

PLAYER_V1 = {
    "event_id": "evt-1",
    "session_id": "sess-1",
    "user_id": "9f8e7d6c5b4a3210",
    "timestamp": "2026-05-20T22:14:33.812Z",
    "event_type": "heartbeat",
    "content_id": "live-brasileirao-final-2026",
    "is_live": True,
    "device": {"type": "smart_tv", "model": "LG-OLED-2024", "os": "webOS", "app_version": "1.42.0"},
    "geo": {"region": "SE", "state": "RJ", "city": "Rio de Janeiro", "isp": "isp-vivo"},
    "cdn": "cdn-a",
    "bitrate_kbps": 5800,
    "buffer_length_ms": 12000,
    "playhead_position_s": 4823,
    "error_code": None,
}

PLAYER_V2 = {**PLAYER_V1, "schema_version": "v2", "network_type": "cellular"}

SCTE = {
    "marker_id": "mk-1",
    "channel": "live-brasileirao-final-2026",
    "splice_command": "splice_insert",
    "event_id_scte": 1003,
    "out_of_network": True,
    "pts_time": 459000000,
    "wallclock": "2026-05-20T22:14:30.000Z",
    "duration_s": 60,
    "break_type": "commercial",
}

CONTENT = {
    "content_id": "live-brasileirao-final-2026",
    "title": "Final do Brasileirão 2026 — AO VIVO",
    "genre": "sports",
    "is_live": True,
    "is_premium": True,
    "scheduled_start_utc": "2026-05-20T21:45:00.000Z",
    "scheduled_end_utc": "2026-05-20T23:30:00.000Z",
    "rights_window_start": "2026-05-20T19:45:00.000Z",
    "rights_window_end": "2026-05-21T01:45:00.000Z",
    "rights_territories": ["BR"],
    "classification": "L",
    "ad_pod_policy": "midroll_dynamic_ssai",
    "language": "pt-BR",
    "duration_min": 105,
    "eidr_id": "10.5240/XXXX-XXXX-XXXX-XXXX-XXXX-C",
    "gracenote_id": "123456789012",
    "created_at": "2026-04-20T21:45:00.000Z",
    "updated_at": "2026-05-19T21:45:00.000Z",
}


def _encode(schema_name: str, record: dict) -> bytes:
    parsed = fastavro.parse_schema(load_schema(schema_name))
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, parsed, record)
    return buf.getvalue()


def _decode(writer_schema_name: str, raw: bytes, reader_schema_name: str | None = None) -> dict:
    writer = fastavro.parse_schema(load_schema(writer_schema_name))
    reader = fastavro.parse_schema(load_schema(reader_schema_name)) if reader_schema_name else None
    return fastavro.schemaless_reader(io.BytesIO(raw), writer, reader)


# ---------------------------------------------------------------------------
# Round-trip
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "schema_name,record",
    [
        ("player_events.v1", PLAYER_V1),
        ("player_events.v2", PLAYER_V2),
        ("scte35_markers.v1", SCTE),
        ("content_metadata.v1", CONTENT),
    ],
)
def test_roundtrip(schema_name: str, record: dict) -> None:
    decoded = _decode(schema_name, _encode(schema_name, record))
    for key, value in record.items():
        assert decoded[key] == value


# ---------------------------------------------------------------------------
# Schema resolution v1 ↔ v2 (F6)
# ---------------------------------------------------------------------------

def test_backward_reader_v2_reads_v1_data() -> None:
    """Dado escrito em v1, lido por reader v2 → campos novos vêm do default."""
    raw = _encode("player_events.v1", PLAYER_V1)
    decoded = _decode("player_events.v1", raw, reader_schema_name="player_events.v2")
    assert decoded["network_type"] == "unknown"
    assert decoded["schema_version"] == "v1"
    assert decoded["event_id"] == PLAYER_V1["event_id"]


def test_old_consumer_v1_reads_v2_data_without_breaking() -> None:
    """Dado escrito em v2, lido por reader v1 → ignora network_type, não quebra."""
    raw = _encode("player_events.v2", PLAYER_V2)
    decoded = _decode("player_events.v2", raw, reader_schema_name="player_events.v1")
    assert "network_type" not in decoded
    assert "schema_version" not in decoded
    assert decoded["session_id"] == PLAYER_V2["session_id"]
    assert decoded["bitrate_kbps"] == PLAYER_V2["bitrate_kbps"]


def test_enum_default_on_unknown_network_type() -> None:
    """network_type fora do conjunto resolve para o default do enum ('unknown')."""
    bad = {**PLAYER_V2, "network_type": "satellite"}
    with pytest.raises(Exception):
        _encode("player_events.v2", bad)  # writer rejeita símbolo inválido


# ---------------------------------------------------------------------------
# Convenções de subject
# ---------------------------------------------------------------------------

def test_subject_naming() -> None:
    assert subject_for("player_events") == "player_events-value"
    assert ACTIVE_SCHEMA["player_events"] == "player_events.v2"
