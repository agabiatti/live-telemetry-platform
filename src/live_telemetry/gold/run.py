"""Orquestrador do Gold (batch, one-shot).

Fluxo:
  1. Bridge: carrega Bronze Delta (player/scte/content) + ad_decisions cru → tabelas DuckDB.
  2. dbt run + dbt test (staging → marts), lendo essas fontes.
  3. Export dos marts para Parquet particionado + manifesto `_gold_runs.jsonl`.

Idempotência: DuckDB é recriado do zero a cada run a partir do Bronze determinístico →
re-rodar produz Gold idêntica (checksum no manifesto comprova). Ver ADR-003.

Bridge Delta→DuckDB via python (deltalake) em vez da extensão delta do DuckDB: evita
dependência de instalação de extensão em runtime (offline-friendly).

Uso: python -m live_telemetry.gold.run
"""

from __future__ import annotations

import json
import os
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from deltalake import DeltaTable

from live_telemetry.common.config import load_config
from live_telemetry.common.logging import configure_logging, get_logger

log = get_logger(__name__)

DBT_DIR = os.environ.get("DBT_DIR", "dbt")
MARTS = ["gold_session_window_qoe", "gold_ccv", "gold_ad_impact", "gold_ad_creatives"]


def _load_delta(con: duckdb.DuckDBPyConnection, path: str, table: str) -> int:
    arrow = DeltaTable(path).to_pyarrow_table()
    con.register(f"{table}__arrow", arrow)
    con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM {table}__arrow")
    con.unregister(f"{table}__arrow")
    return arrow.num_rows


def bridge(cfg, duckdb_path: str) -> int:
    """Carrega Bronze + ad_decisions em tabelas DuckDB. Retorna a versão Delta do Bronze player."""
    bronze = cfg.paths.bronze
    con = duckdb.connect(duckdb_path)
    try:
        _load_delta(con, os.path.join(bronze, "player_events"), "src_player_events")
        _load_delta(con, os.path.join(bronze, "scte35_markers"), "src_scte35_markers")
        _load_delta(con, os.path.join(bronze, "content_metadata"), "src_content_metadata")

        ad_path = os.path.join(cfg.paths.raw, "ad_decisions.jsonl")
        if os.path.exists(ad_path):
            con.execute(
                f"CREATE OR REPLACE TABLE src_ad_decisions AS "
                f"SELECT * FROM read_json_auto('{ad_path}')"
            )
        else:
            con.execute(
                "CREATE TABLE src_ad_decisions(event_id_scte INTEGER, price_cpm_brl DOUBLE)"
            )
        log.info("bridge_loaded", path=duckdb_path)
    finally:
        con.close()
    return DeltaTable(os.path.join(bronze, "player_events")).version()


def run_dbt(duckdb_path: str) -> None:
    env = dict(os.environ, GOLD_DUCKDB=duckdb_path)
    for sub in ("run", "test"):
        result = subprocess.run(
            ["dbt", sub, "--project-dir", DBT_DIR, "--profiles-dir", DBT_DIR],
            env=env, capture_output=True, text=True,
        )
        log.info("dbt_phase", phase=sub, returncode=result.returncode)
        print(result.stdout[-1500:])
        if result.returncode != 0:
            print(result.stderr[-1500:])
            if sub == "run":
                raise RuntimeError("dbt run falhou")


def export_and_manifest(cfg, duckdb_path: str, bronze_version: int) -> dict:
    gold_dir = cfg.paths.gold
    con = duckdb.connect(duckdb_path)
    try:
        # Export particionado (qoe por hora) + flat para os demais.
        qoe_dir = os.path.join(gold_dir, "gold_session_window_qoe")
        con.execute(
            f"COPY (SELECT * FROM gold_session_window_qoe) TO '{qoe_dir}' "
            f"(FORMAT PARQUET, PARTITION_BY (window_hour), OVERWRITE_OR_IGNORE)"
        )
        for mart in ("gold_ccv", "gold_ad_impact", "gold_ad_creatives"):
            con.execute(
                f"COPY (SELECT * FROM {mart}) TO '{os.path.join(gold_dir, mart + '.parquet')}' "
                f"(FORMAT PARQUET)"
            )

        # Checksum determinístico das métricas-chave do QoE (prova de idempotência).
        qoe_checksum = con.execute(
            "SELECT md5(string_agg(concat_ws('|', window_key, events::VARCHAR, "
            "error_count::VARCHAR, rebuffer_ms::VARCHAR, round(rebuffering_ratio, 6)::VARCHAR), "
            "'' ORDER BY window_key)) FROM gold_session_window_qoe"
        ).fetchone()[0]
        counts = {m: con.execute(f"SELECT count(*) FROM {m}").fetchone()[0] for m in MARTS}
    finally:
        con.close()

    record = {
        "run_id": str(uuid.uuid4()),
        "run_at": datetime.now(timezone.utc).isoformat(),
        "bronze_player_version": bronze_version,
        "rows": counts,
        "qoe_checksum": qoe_checksum,
    }
    manifest_path = os.path.join(gold_dir, "_gold_runs.jsonl")
    with open(manifest_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")
    log.info("gold_manifest", **record)
    return record


def main() -> int:
    configure_logging()
    cfg = load_config()
    gold_dir = cfg.paths.gold
    Path(gold_dir).mkdir(parents=True, exist_ok=True)
    duckdb_path = os.path.join(gold_dir, "gold.duckdb")
    if os.path.exists(duckdb_path):
        os.remove(duckdb_path)  # rebuild determinístico do zero

    log.info("gold_start", bronze=cfg.paths.bronze, gold=gold_dir)
    bronze_version = bridge(cfg, duckdb_path)
    run_dbt(duckdb_path)
    export_and_manifest(cfg, duckdb_path, bronze_version)
    log.info("gold_done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
