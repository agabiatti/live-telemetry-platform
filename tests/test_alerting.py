"""Testes do detector multi-burn-rate (lógica pura, sem Bytewax)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from live_telemetry.silver.alerting import BurnRateConfig, step

T0 = datetime(2026, 5, 20, 22, 0, tzinfo=timezone.utc)


def _feed(seq: list[tuple[int, int, int]], cfg: BurnRateConfig, cdn: str = "cdn-b") -> list[dict]:
    state = None
    transitions = []
    for minute, total, bad in seq:
        ws = T0 + timedelta(minutes=minute)
        state, tr = step(state, cdn, ws, total, bad, total, cfg)
        if tr:
            transitions.append(tr)
    return transitions


def test_burst_fires_then_resolves() -> None:
    cfg = BurnRateConfig(slow_long_min=10, clear_consecutive=2)
    seq = (
        [(m, 100, 0) for m in range(0, 5)]      # baseline saudável
        + [(m, 100, 10) for m in range(5, 9)]   # burst: 10% error (burn=10)
        + [(m, 100, 0) for m in range(9, 20)]   # recuperação
    )
    states = [t["state"] for t in _feed(seq, cfg)]
    assert "FIRING" in states
    assert "RESOLVED" in states
    assert states.index("FIRING") < states.index("RESOLVED")


def test_healthy_never_fires() -> None:
    cfg = BurnRateConfig()
    seq = [(m, 100, 0) for m in range(0, 30)]
    assert _feed(seq, cfg) == []


def test_fires_for_any_cdn_not_hardcoded() -> None:
    cfg = BurnRateConfig(slow_long_min=10, clear_consecutive=2)
    seq = [(m, 100, 0) for m in range(0, 5)] + [(m, 100, 12) for m in range(5, 9)]
    transitions = _feed(seq, cfg, cdn="cdn-a")
    firing = [t for t in transitions if t["state"] == "FIRING"]
    assert firing and firing[0]["cdn"] == "cdn-a"
    assert firing[0]["tier"] == "page"
