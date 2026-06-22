"""Definições de métrica QoE/audiência — FONTE ÚNICA.

Silver (Bytewax, Python) importa daqui. Gold (dbt/SQL) **espelha** estas fórmulas; um teste
de cross-check valida que ambas produzem os mesmos números contra a mesma fixture. Centralizar
as fórmulas aqui mitiga a duplicação de lógica do Lambda (ver DESIGN §7).

Grão do acumulador: (session_id, janela de 1 min). Métricas derivadas em `finalize()`.

Fórmulas (espelhar no Gold):
- rebuffering_ratio = rebuffer_ms / (rebuffer_ms + watch_ms)
- avg_bitrate_kbps  = média de bitrate_kbps nos heartbeats
- error_rate        = error_events / total_events
- exit_before_video_start = teve término/erro sem nenhum video_start na janela
- ccv (downstream)  = COUNT(DISTINCT session_id) por janela × dimensão
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

REBUFFER_EVENTS = ("buffer_start", "buffer_end")
PLAYING_EVENTS = ("heartbeat", "video_start", "bitrate_switch")


@dataclass
class QoEAcc:
    """Acumulador por (session_id, janela). Folder é associativo o suficiente para o merge
    de janelas de sessão (markers raros). Eventos chegam ordenados por event-time (ordered=True)."""

    session_id: str = ""
    region: Optional[str] = None
    device_type: Optional[str] = None
    cdn: Optional[str] = None

    events: int = 0
    by_type: dict[str, int] = field(default_factory=dict)

    bitrate_sum: int = 0
    bitrate_n: int = 0

    rebuffer_ms: int = 0
    _open_buffer_ts: Optional[datetime] = None

    error_count: int = 0
    had_video_start: bool = False
    had_terminal: bool = False  # video_end ou error

    first_ts: Optional[datetime] = None
    last_ts: Optional[datetime] = None


def build_acc() -> QoEAcc:
    return QoEAcc()


def fold_event(acc: QoEAcc, event: dict[str, Any]) -> QoEAcc:
    """Folder do fold_window. `event` é o dict decodificado (event_time: datetime)."""
    et: datetime = event["event_time"]
    etype: str = event["event_type"]

    if not acc.session_id:
        acc.session_id = event["session_id"]
    # Dimensões: primeira não-nula vista (estáveis na sessão).
    acc.region = acc.region or event.get("geo_region")
    acc.device_type = acc.device_type or event.get("device_type")
    acc.cdn = acc.cdn or event.get("cdn")

    acc.events += 1
    acc.by_type[etype] = acc.by_type.get(etype, 0) + 1
    acc.first_ts = et if acc.first_ts is None else min(acc.first_ts, et)
    acc.last_ts = et if acc.last_ts is None else max(acc.last_ts, et)

    # Bitrate médio: amostrado nos eventos "tocando".
    if etype in PLAYING_EVENTS and event.get("bitrate_kbps"):
        acc.bitrate_sum += int(event["bitrate_kbps"])
        acc.bitrate_n += 1

    # Rebuffering: pareia buffer_start → buffer_end (ordered=True garante ordem).
    if etype == "buffer_start":
        acc._open_buffer_ts = et
    elif etype == "buffer_end" and acc._open_buffer_ts is not None:
        delta_ms = int((et - acc._open_buffer_ts).total_seconds() * 1000)
        if delta_ms > 0:
            acc.rebuffer_ms += delta_ms
        acc._open_buffer_ts = None

    if etype == "error":
        acc.error_count += 1
        acc.had_terminal = True
    if etype == "video_start":
        acc.had_video_start = True
    if etype == "video_end":
        acc.had_terminal = True

    return acc


def merge_acc(a: QoEAcc, b: QoEAcc) -> QoEAcc:
    """Merge de janelas (usado pelo windower em merges raros). Soma contadores."""
    a.events += b.events
    for k, v in b.by_type.items():
        a.by_type[k] = a.by_type.get(k, 0) + v
    a.bitrate_sum += b.bitrate_sum
    a.bitrate_n += b.bitrate_n
    a.rebuffer_ms += b.rebuffer_ms
    a.error_count += b.error_count
    a.had_video_start = a.had_video_start or b.had_video_start
    a.had_terminal = a.had_terminal or b.had_terminal
    a.region = a.region or b.region
    a.device_type = a.device_type or b.device_type
    a.cdn = a.cdn or b.cdn
    ts = [t for t in (a.first_ts, b.first_ts) if t]
    a.first_ts = min(ts) if ts else None
    ts = [t for t in (a.last_ts, b.last_ts) if t]
    a.last_ts = max(ts) if ts else None
    return a


def finalize(acc: QoEAcc) -> dict[str, Any]:
    """Deriva as métricas QoE finais do acumulador."""
    watch_ms = 0
    if acc.first_ts and acc.last_ts:
        watch_ms = max(0, int((acc.last_ts - acc.first_ts).total_seconds() * 1000) - acc.rebuffer_ms)

    denom = acc.rebuffer_ms + watch_ms
    rebuffering_ratio = acc.rebuffer_ms / denom if denom > 0 else 0.0
    avg_bitrate = acc.bitrate_sum / acc.bitrate_n if acc.bitrate_n else 0.0
    error_rate = acc.error_count / acc.events if acc.events else 0.0
    exit_before_start = acc.had_terminal and not acc.had_video_start

    return {
        "session_id": acc.session_id,
        "geo_region": acc.region,
        "device_type": acc.device_type,
        "cdn": acc.cdn,
        "events": acc.events,
        "rebuffer_ms": acc.rebuffer_ms,
        "watch_ms": watch_ms,
        "rebuffering_ratio": round(rebuffering_ratio, 6),
        "avg_bitrate_kbps": round(avg_bitrate, 2),
        "error_count": acc.error_count,
        "error_rate": round(error_rate, 6),
        "buffer_start_count": acc.by_type.get("buffer_start", 0),
        "had_video_start": acc.had_video_start,
        "exit_before_video_start": exit_before_start,
    }
