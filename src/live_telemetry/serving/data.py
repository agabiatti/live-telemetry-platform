"""Camada de dados do dashboard — queries puras (testáveis headless, sem Streamlit).

Lê Gold (Parquet) e Silver/alertas (Delta) via DuckDB. Funções retornam estruturas simples
(listas de dicts / DuckDB relations materializadas) para o app só renderizar.
"""

from __future__ import annotations

import os
from typing import Any

import duckdb
from deltalake import DeltaTable

from live_telemetry.common.config import Config


def _has_delta(path: str) -> bool:
    return os.path.exists(os.path.join(path, "_delta_log"))


def _con(cfg: Config) -> duckdb.DuckDBPyConnection:
    """Conexão DuckDB com Silver/alertas (Delta) registrados; Gold lido via Parquet."""
    con = duckdb.connect()
    silver_qoe = os.path.join(cfg.paths.silver, "session_window_qoe")
    alerts = os.path.join(cfg.paths.silver, "cdn_alerts")
    if _has_delta(silver_qoe):
        # Sink é append-only (throughput): re-emits de recovery podem duplicar (session_id,
        # window_start). Dedup no read mantendo o processed_at mais novo → idempotência lógica
        # sem MERGE no hot path. View 'silver_qoe' deduplicada substitui o acesso cru.
        con.register("silver_qoe_raw", DeltaTable(silver_qoe).to_pyarrow_table())
        con.execute(
            "CREATE VIEW silver_qoe AS "
            "SELECT * EXCLUDE (rn) FROM ("
            "  SELECT *, row_number() OVER ("
            "    PARTITION BY session_id, window_start ORDER BY processed_at DESC"
            "  ) AS rn FROM silver_qoe_raw"
            ") WHERE rn = 1"
        )
    if _has_delta(alerts):
        con.register("cdn_alerts", DeltaTable(alerts).to_pyarrow_table())
    return con


def current_alerts(cfg: Config) -> list[dict[str, Any]]:
    """Estado atual por CDN = última transição registrada."""
    con = _con(cfg)
    if "cdn_alerts" not in [r[0] for r in con.execute("SHOW TABLES").fetchall()]:
        return []
    rows = con.execute(
        "SELECT cdn, last(state ORDER BY minute) AS state, last(tier ORDER BY minute) AS tier "
        "FROM cdn_alerts GROUP BY cdn ORDER BY cdn"
    ).fetchall()
    return [{"cdn": r[0], "state": r[1], "tier": r[2]} for r in rows]


def ccv_by_region(cfg: Config) -> list[dict[str, Any]]:
    """CCV (pico) por região, da Gold."""
    path = os.path.join(cfg.paths.gold, "gold_ccv.parquet")
    if not os.path.exists(path):
        return []
    con = duckdb.connect()
    rows = con.execute(
        f"SELECT geo_region, max(ccv) AS peak_ccv, round(avg(ccv), 1) AS avg_ccv "
        f"FROM read_parquet('{path}') GROUP BY geo_region ORDER BY peak_ccv DESC"
    ).fetchall()
    return [{"region": r[0], "peak_ccv": r[1], "avg_ccv": r[2]} for r in rows]


def rebuffering_by_cdn(cfg: Config) -> list[dict[str, Any]]:
    """Série temporal de rebuffering ratio por CDN (agregado por janela), da Silver."""
    con = _con(cfg)
    if "silver_qoe" not in [r[0] for r in con.execute("SHOW TABLES").fetchall()]:
        return []
    rows = con.execute(
        "SELECT window_start, cdn, "
        "sum(rebuffer_ms)::double / nullif(sum(rebuffer_ms + watch_ms), 0) AS rebuffering_ratio "
        "FROM silver_qoe GROUP BY window_start, cdn ORDER BY window_start"
    ).fetchall()
    return [{"window_start": r[0], "cdn": r[1], "rebuffering_ratio": r[2] or 0.0} for r in rows]


def unique_views_per_minute(cfg: Config) -> list[dict[str, Any]]:
    """Views únicas por minuto, da Silver (near real-time).

    Cada linha do silver_qoe é (session_id, window_start). Contamos session_id distintos
    truncando a janela tumbling ao minuto — proxy de audiência única por minuto. Speed layer,
    então reflete o estado corrente das janelas materializadas."""
    con = _con(cfg)
    if "silver_qoe" not in [r[0] for r in con.execute("SHOW TABLES").fetchall()]:
        return []
    rows = con.execute(
        "SELECT date_trunc('minute', window_start) AS minute, "
        "count(DISTINCT session_id) AS unique_views "
        "FROM silver_qoe GROUP BY minute ORDER BY minute"
    ).fetchall()
    return [{"minute": r[0], "unique_views": r[1]} for r in rows]


def avg_bitrate_by_cdn(cfg: Config) -> list[dict[str, Any]]:
    """Bitrate médio entregue por CDN ao longo do tempo, da Silver — qualidade percebida.

    Série temporal por janela: queda de bitrate antecede rebuffering em rede ruim/throttle.
    Ponderado por watch_ms (tempo assistido) pra refletir experiência real, não média simples
    de janelas curtas."""
    con = _con(cfg)
    if "silver_qoe" not in [r[0] for r in con.execute("SHOW TABLES").fetchall()]:
        return []
    rows = con.execute(
        "SELECT window_start, cdn, "
        "sum(avg_bitrate_kbps * watch_ms)::double / nullif(sum(watch_ms), 0) AS avg_bitrate_kbps "
        "FROM silver_qoe WHERE watch_ms > 0 "
        "GROUP BY window_start, cdn ORDER BY window_start"
    ).fetchall()
    return [{"window_start": r[0], "cdn": r[1], "avg_bitrate_kbps": r[2] or 0.0} for r in rows]


def qoe_by_network_type(cfg: Config) -> list[dict[str, Any]]:
    """QoE agregada por network_type, da Gold (F6: dimensão que veio do schema v2).

    Prova a cadeia de schema evolution entregando valor analítico: dado v1 (sem o campo)
    aparece como 'unknown' (default-fill no staging) ao lado dos valores reais do v2,
    sem quebrar a query. Painel some graciosamente se a coluna não existir (Gold antiga)."""
    qoe_dir = os.path.join(cfg.paths.gold, "gold_session_window_qoe")
    if not os.path.isdir(qoe_dir):
        return []
    glob = os.path.join(qoe_dir, "**", "*.parquet")  # export particionado por window_hour
    con = duckdb.connect()
    cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{glob}')").fetchall()]
    if "network_type" not in cols:
        return []
    rows = con.execute(
        f"SELECT network_type, count(*) AS windows, "
        f"sum(rebuffer_ms)::double / nullif(sum(rebuffer_ms + watch_ms), 0) AS rebuffering_ratio, "
        f"round(avg(avg_bitrate_kbps), 0) AS avg_bitrate_kbps "
        f"FROM read_parquet('{glob}') GROUP BY network_type ORDER BY windows DESC"
    ).fetchall()
    return [
        {"network_type": r[0], "windows": r[1], "rebuffering_ratio": r[2] or 0.0, "avg_bitrate_kbps": r[3] or 0.0}
        for r in rows
    ]


def ad_impact(cfg: Config) -> list[dict[str, Any]]:
    """Impacto comercial por marker SCTE-35, da Gold."""
    path = os.path.join(cfg.paths.gold, "gold_ad_impact.parquet")
    if not os.path.exists(path):
        return []
    con = duckdb.connect()
    rows = con.execute(
        f"SELECT event_id_scte, break_type, active_sessions, ad_impressions, "
        f"sum_cpm_brl, est_revenue_brl FROM read_parquet('{path}') "
        f"ORDER BY est_revenue_brl DESC"
    ).fetchall()
    cols = ["event_id_scte", "break_type", "active_sessions", "ad_impressions", "sum_cpm_brl", "est_revenue_brl"]
    return [dict(zip(cols, r)) for r in rows]


def ad_creatives(cfg: Config) -> list[dict[str, Any]]:
    """Detalhe de criativos por break SCTE-35: qual anunciante/criativo rodou, da Gold.

    Responde 'qual ad rodou, nome e empresa' por marker. advertiser_name = empresa;
    creative_id = o criativo (sem nome humano na fonte). Some gracioso se o mart não existir
    (Gold antiga, antes do gold_ad_creatives)."""
    path = os.path.join(cfg.paths.gold, "gold_ad_creatives.parquet")
    if not os.path.exists(path):
        return []
    con = duckdb.connect()
    rows = con.execute(
        f"SELECT event_id_scte, break_type, advertiser_name, advertiser_id, "
        f"creative_id::VARCHAR AS creative_id, "
        f"creative_duration_s, ad_impressions, sum_cpm_brl, est_revenue_brl "
        f"FROM read_parquet('{path}') ORDER BY est_revenue_brl DESC"
    ).fetchall()
    cols = ["event_id_scte", "break_type", "advertiser_name", "advertiser_id", "creative_id",
            "creative_duration_s", "ad_impressions", "sum_cpm_brl", "est_revenue_brl"]
    return [dict(zip(cols, r)) for r in rows]
