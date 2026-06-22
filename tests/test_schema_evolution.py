"""Propagação do schema evolution (F6): contrato v1/v2 → Bronze → Gold (staging + mart).

Roda o SQL REAL do dbt (`stg_player_events.sql` + `gold_session_window_qoe.sql`) contra DuckDB
— resolvendo os refs jinja manualmente — para provar a cadeia end-to-end, não uma cópia da SQL:

  - dado v1 (sem `network_type`) → Bronze guarda NULL (cru) → staging aplica default-fill
    (`coalesce -> 'unknown'`) → mart carrega 'unknown'. v1 NÃO quebra sob o novo schema.
  - dado v2 (com `network_type`) → valor preservado em todo o caminho até o mart.

Silver fica FORA desta cadeia por design (speed layer de QoE não precisa da dimensão;
network_type é dimensão de analytics batch). Ver docs/schema-evolution.md.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

from live_telemetry.bronze.transform import flatten_player

REPO = Path(__file__).resolve().parents[1]
STG_SQL = REPO / "dbt" / "models" / "staging" / "stg_player_events.sql"
MART_SQL = REPO / "dbt" / "models" / "marts" / "gold_session_window_qoe.sql"

KMETA = {"partition": 0, "offset": 1, "ingested_at": datetime(2026, 5, 20, 22, 14, 34, tzinfo=timezone.utc)}

# Espelham a forma dos contratos CONTRACTS/player_events.v{1,2}.avsc.
_BASE = {
    "event_id": "e0", "session_id": "s0", "user_id": "u0",
    "timestamp": "2026-05-20T22:14:33.000Z", "event_type": "video_start",
    "content_id": "live-brasileirao-final-2026", "is_live": True,
    "device": {"type": "smart_tv", "model": "LG", "os": "webOS", "app_version": "1.0"},
    "geo": {"region": "SE", "state": "RJ", "city": "Rio", "isp": "isp-x"},
    "cdn": "cdn-a", "bitrate_kbps": 5000, "buffer_length_ms": 0,
    "playhead_position_s": 1, "error_code": None,
}


def _player_v1(session: str, offset: int) -> dict:
    rec = {**_BASE, "session_id": session, "event_id": f"{session}-{offset}"}
    return flatten_player(rec, {**KMETA, "offset": offset})  # sem network_type/schema_version


def _player_v2(session: str, offset: int, net: str) -> dict:
    rec = {**_BASE, "session_id": session, "event_id": f"{session}-{offset}",
           "schema_version": "v2", "network_type": net}
    return flatten_player(rec, {**KMETA, "offset": offset})


def _resolve(sql: str) -> str:
    """Resolve os refs jinja do dbt para nomes de tabela DuckDB concretos."""
    sql = re.sub(r"\{\{\s*source\(\s*'bronze'\s*,\s*'src_player_events'\s*\)\s*\}\}",
                 "src_player_events", sql)
    sql = re.sub(r"\{\{\s*ref\(\s*'stg_player_events'\s*\)\s*\}\}",
                 "stg_player_events", sql)
    sql = re.sub(r"\{\{\s*ref\(\s*'stg_content'\s*\)\s*\}\}",
                 "stg_content", sql)
    return sql


def test_network_type_propagates_contract_bronze_gold() -> None:
    # Uma sessão v1 (sem o campo) e uma v2 (cellular). 2 eventos cada p/ formar janela.
    rows = (
        [_player_v1("sess-v1", i) for i in (1, 2)]
        + [_player_v2("sess-v2", i, "cellular") for i in (3, 4)]
    )
    con = duckdb.connect()
    con.register("src_player_events", pd.DataFrame(rows))

    con.execute(f"CREATE TABLE stg_player_events AS {_resolve(STG_SQL.read_text())}")
    # stg_content mínimo p/ o join SCD do mart (content_id casa com _BASE).
    con.register("stg_content", pd.DataFrame([
        {"content_id": "live-brasileirao-final-2026", "title": "Final", "genre": "sports", "is_premium": True},
    ]))

    # Bronze guardou o cru: v1 sem network_type (NULL), v2 com valor.
    stg = {r[0]: r[1] for r in con.execute(
        "SELECT session_id, network_type FROM stg_player_events "
        "WHERE event_type='video_start' GROUP BY 1,2"
    ).fetchall()}
    assert stg["sess-v1"] == "unknown"   # default-fill aplicado no staging do Gold
    assert stg["sess-v2"] == "cellular"  # valor v2 preservado

    mart = con.execute(_resolve(MART_SQL.read_text())).df()
    by_session = mart.set_index("session_id")["network_type"].to_dict()
    assert by_session["sess-v1"] == "unknown"   # cadeia chega ao mart sem quebrar v1
    assert by_session["sess-v2"] == "cellular"  # dimensão v2 entregue no Gold


def test_network_type_reaches_dashboard(tmp_path, monkeypatch) -> None:
    """Último elo: Gold (mart particionado) → query do dashboard agrega por network_type."""
    from live_telemetry.common.config import load_config
    from live_telemetry.serving import data

    # Escreve um mart mínimo no layout de export real (particionado por window_hour).
    part = tmp_path / "gold_session_window_qoe" / "window_hour=2026-05-20T22"
    part.mkdir(parents=True)
    pd.DataFrame([
        {"network_type": "unknown", "rebuffer_ms": 0, "watch_ms": 1000, "avg_bitrate_kbps": 4000.0},
        {"network_type": "cellular", "rebuffer_ms": 100, "watch_ms": 900, "avg_bitrate_kbps": 3000.0},
    ]).to_parquet(part / "data_0.parquet")

    monkeypatch.setenv("GOLD_PATH", str(tmp_path))
    cfg = load_config()
    out = {r["network_type"]: r for r in data.qoe_by_network_type(cfg)}
    assert set(out) == {"unknown", "cellular"}            # v1 (unknown) ao lado de v2
    assert out["cellular"]["rebuffering_ratio"] == 0.1    # 100/(100+900)
