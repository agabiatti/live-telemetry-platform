"""Testes da lógica pura do Silver — métricas QoE e dedupe (sem Bytewax/broker)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from live_telemetry.common.metrics import build_acc, finalize, fold_event, merge_acc
from live_telemetry.silver.dedupe import dedupe_step

T0 = datetime(2026, 5, 20, 22, 0, 0, tzinfo=timezone.utc)


def _evt(etype: str, secs: int, **kw) -> dict:
    base = {
        "event_id": kw.get("event_id", f"{etype}-{secs}"),
        "session_id": "s1",
        "event_type": etype,
        "event_time": T0 + timedelta(seconds=secs),
        "geo_region": "SE",
        "device_type": "smart_tv",
        "cdn": "cdn-a",
        "bitrate_kbps": kw.get("bitrate", 5000),
    }
    base.update({k: v for k, v in kw.items() if k not in ("event_id", "bitrate")})
    return base


# --------------------------------------------------------------- métricas

def test_rebuffering_ratio_from_buffer_pair() -> None:
    acc = build_acc()
    for e in [
        _evt("video_start", 0),
        _evt("heartbeat", 10),
        _evt("buffer_start", 12),
        _evt("buffer_end", 14),   # 2s de rebuffer
        _evt("heartbeat", 20),
    ]:
        acc = fold_event(acc, e)
    out = finalize(acc)
    assert out["rebuffer_ms"] == 2000
    assert 0 < out["rebuffering_ratio"] < 1
    assert out["had_video_start"] is True
    assert out["exit_before_video_start"] is False


def test_avg_bitrate_and_error_rate() -> None:
    acc = build_acc()
    for e in [
        _evt("video_start", 0, bitrate=4000),
        _evt("heartbeat", 10, bitrate=6000),
        _evt("error", 12, bitrate=6000),
    ]:
        acc = fold_event(acc, e)
    out = finalize(acc)
    assert out["avg_bitrate_kbps"] == 5000.0  # (4000+6000)/2 (error não conta como playing)
    assert out["error_count"] == 1
    assert round(out["error_rate"], 4) == round(1 / 3, 4)


def test_exit_before_video_start() -> None:
    acc = build_acc()
    for e in [_evt("buffer_start", 1), _evt("error", 2)]:
        acc = fold_event(acc, e)
    out = finalize(acc)
    assert out["exit_before_video_start"] is True


def test_merge_acc_sums() -> None:
    a = build_acc()
    b = build_acc()
    a = fold_event(a, _evt("heartbeat", 0))
    b = fold_event(b, _evt("heartbeat", 5))
    merged = merge_acc(a, b)
    assert merged.events == 2


# --------------------------------------------------------------- dedupe

def test_dedupe_detects_duplicate() -> None:
    state = None
    e = _evt("heartbeat", 0, event_id="dup-1")
    state, (_, is_dup1) = dedupe_step(state, e, horizon_s=180)
    state, (_, is_dup2) = dedupe_step(state, dict(e), horizon_s=180)
    assert is_dup1 is False
    assert is_dup2 is True


def test_dedupe_horizon_evicts_old() -> None:
    state = None
    old = _evt("heartbeat", 0, event_id="old")
    state, _ = dedupe_step(state, old, horizon_s=60)
    # evento 200s depois → "old" sai do horizonte de 60s
    new = _evt("heartbeat", 200, event_id="new")
    state, _ = dedupe_step(state, new, horizon_s=60)
    assert "old" not in state
    assert "new" in state
