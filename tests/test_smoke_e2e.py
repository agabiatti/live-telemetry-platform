"""Smoke e2e da cadeia gerador→producer→bronze→gold (passa pelo broker de verdade).

NÃO roda na suíte unit (pula sem `SMOKE_E2E=1`): exige a stack já executada — o producer
publicou no Redpanda, o Bronze drenou os tópicos e o Gold materializou os marts. O alvo
`make smoke` orquestra a stack e então roda este arquivo com a env ligada.

O que prova (que os unit tests NÃO cobrem):
  - dado FLUIU pelo broker: Bronze tem linhas ⇒ producer publicou e Bronze drenou via Kafka.
  - coexistência v1/v2 NO FIO: Bronze tem linhas v1 (network_type NULL) e v2 (não-nulo),
    resolvidas pelo schema-id do wire-format.
  - Gold materializou com a dimensão v2 (network_type, com bucket 'unknown' do default-fill).
  - manifesto determinístico do Gold (_gold_runs.jsonl) bate com as linhas do mart.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import duckdb
import pytest
from deltalake import DeltaTable

from live_telemetry.common.config import load_config

pytestmark = pytest.mark.skipif(
    os.getenv("SMOKE_E2E") != "1",
    reason="smoke e2e: requer a stack rodada (use `make smoke`).",
)


@pytest.fixture(scope="module")
def cfg():
    return load_config()


def _bronze_con(cfg) -> duckdb.DuckDBPyConnection:
    path = os.path.join(cfg.paths.bronze, "player_events")
    assert os.path.exists(os.path.join(path, "_delta_log")), f"Bronze ausente em {path} (rode producer+bronze)"
    con = duckdb.connect()
    con.register("bronze_player", DeltaTable(path).to_pyarrow_table())
    return con


def test_data_flowed_through_broker(cfg) -> None:
    """Bronze com linhas = producer publicou no Redpanda e Bronze drenou o tópico."""
    con = _bronze_con(cfg)
    n = con.execute("SELECT count(*) FROM bronze_player").fetchone()[0]
    assert n > 0, "Bronze vazio: a cadeia gerador→producer→bronze não fluiu pelo broker"


def test_schema_v1_v2_coexist_on_wire(cfg) -> None:
    """Schema-id no fio resolvido: convivem mensagens v1 (network_type NULL) e v2 (não-nulo)."""
    con = _bronze_con(cfg)
    v1 = con.execute("SELECT count(*) FROM bronze_player WHERE network_type IS NULL").fetchone()[0]
    v2 = con.execute("SELECT count(*) FROM bronze_player WHERE network_type IS NOT NULL").fetchone()[0]
    assert v1 > 0, "esperado dado v1 (sem network_type) no Bronze"
    assert v2 > 0, "esperado dado v2 (com network_type) no Bronze — coexistência no fio"


def test_gold_materialized_with_v2_dimension(cfg) -> None:
    """Gold mart tem linhas e a dimensão network_type, com 'unknown' do default-fill (F6)."""
    qoe_dir = os.path.join(cfg.paths.gold, "gold_session_window_qoe")
    glob = os.path.join(qoe_dir, "**", "*.parquet")
    assert os.path.isdir(qoe_dir), f"Gold mart ausente em {qoe_dir} (rode gold)"
    con = duckdb.connect()
    cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{glob}')").fetchall()]
    assert "network_type" in cols, "Gold mart sem coluna network_type (F6 não propagou)"
    dist = dict(con.execute(
        f"SELECT network_type, count(*) FROM read_parquet('{glob}') GROUP BY 1"
    ).fetchall())
    assert sum(dist.values()) > 0, "Gold mart vazio"
    assert dist.get("unknown", 0) > 0, "esperado bucket 'unknown' (default-fill do staging)"


def test_gold_manifest_matches_mart(cfg) -> None:
    """Último run do manifesto Gold tem checksum e bate com a contagem real do mart."""
    manifest = Path(cfg.paths.gold) / "_gold_runs.jsonl"
    assert manifest.exists(), "manifesto _gold_runs.jsonl ausente"
    last = json.loads(manifest.read_text().strip().splitlines()[-1])
    assert last.get("qoe_checksum"), "manifesto sem qoe_checksum"

    glob = os.path.join(cfg.paths.gold, "gold_session_window_qoe", "**", "*.parquet")
    con = duckdb.connect()
    rows = con.execute(f"SELECT count(*) FROM read_parquet('{glob}')").fetchone()[0]
    assert rows == last["rows"]["gold_session_window_qoe"], "linhas do mart divergem do manifesto"
