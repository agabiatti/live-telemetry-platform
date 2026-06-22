"""Dashboard mínimo (Streamlit) — F4.

Duas abas espelhando a arquitetura medallion:
- **Silver** (speed/Bytewax): alerta visual de degradação por CDN + rebuffering rolling.
- **Gold** (batch/dbt): CCV por região, impacto comercial SCTE-35, QoE por network_type
  (dimensão do schema v2 — F6).
Lê Silver (Delta) + Gold (Parquet). A camada de queries (serving/data.py) é pura/testável;
aqui só renderiza.
"""

from __future__ import annotations

import pandas as pd
import streamlit as st

from live_telemetry.common.config import load_config
from live_telemetry.serving import data

st.set_page_config(page_title="Live Telemetry Platform", layout="wide")
cfg = load_config()


def _utc_naive(series: pd.Series) -> pd.Series:
    """Remove o tzinfo mantendo o wall-clock UTC.

    As janelas vêm tz-aware UTC; o Altair/Vega (motor dos charts) reconverte tz-aware pro fuso
    do browser, deslocando o eixo X (ex.: -3h em BRT) e descasando do timestamp do dado (que é
    UTC/`Z`). Deixar tz-naive faz o chart exibir o horário UTC cru, igual ao dado que chega."""
    s = pd.to_datetime(series, utc=True)
    return s.dt.tz_localize(None)


@st.cache_data(ttl=10)
def _load_gold():
    """Batch layer — muda devagar, cache curto basta."""
    return (
        data.ccv_by_region(cfg),
        data.ad_impact(cfg),
        data.qoe_by_network_type(cfg),
        data.ad_creatives(cfg),
    )


st.title("📡 Live Telemetry — Final do Brasileirão 2026")
if st.button("🔄 Atualizar"):
    st.cache_data.clear()

ccv, ads, net_qoe, ad_creatives = _load_gold()

tab_silver, tab_gold = st.tabs(["🟦 Silver — speed layer", "🟨 Gold — batch layer"])

# ====================== ABA SILVER (Bytewax, near real-time) ======================
with tab_silver:
    st.caption(
        "Speed layer (Silver/Bytewax) — alertas e rebuffering near real-time a partir "
        "das janelas de QoE. Auto-atualiza a cada 5 s."
    )

    @st.fragment(run_every=5)
    def _silver_panels() -> None:
        # Lê fresco a cada tick (sem cache) — speed layer reflete estado corrente.
        alerts = data.current_alerts(cfg)
        views_min = data.unique_views_per_minute(cfg)
        rebuffering = data.rebuffering_by_cdn(cfg)
        bitrate = data.avg_bitrate_by_cdn(cfg)

        # --- Alerta de degradação de CDN (dinâmico) ---
        st.subheader("Saúde das CDNs")
        firing = [a for a in alerts if a["state"] == "FIRING"]
        if firing:
            for a in firing:
                st.error(f"🔴 **{a['cdn']}** degradada — alerta **{a['tier']}** ativo")
        elif alerts:
            cols = st.columns(len(alerts))
            for col, a in zip(cols, alerts):
                col.success(f"🟢 {a['cdn']}\n\n{a['state']}")
        else:
            st.info("Sem dados de alerta ainda (suba o Silver).")

        # --- Views únicas por minuto ---
        st.subheader("Views únicas por minuto (UTC)")
        if views_min:
            df = pd.DataFrame(views_min)
            df["minute"] = _utc_naive(df["minute"])
            st.line_chart(df.set_index("minute")["unique_views"])
        else:
            st.info("Sem dados de views ainda (suba o Silver).")

        # --- Rebuffering rolling por CDN ---
        st.subheader("Rebuffering ratio por CDN (UTC)")
        if rebuffering:
            df = pd.DataFrame(rebuffering)
            df["window_start"] = _utc_naive(df["window_start"])
            wide = df.pivot_table(index="window_start", columns="cdn", values="rebuffering_ratio")
            st.line_chart(wide)
        else:
            st.info("Sem dados de rebuffering (suba o Silver).")

        # --- Bitrate médio entregue por CDN (qualidade) ---
        st.subheader("Bitrate médio entregue por CDN (kbps, UTC)")
        if bitrate:
            st.caption(
                "Ponderado por watch_ms. Queda de bitrate antecede rebuffering em rede "
                "ruim/throttle de CDN."
            )
            df = pd.DataFrame(bitrate)
            df["window_start"] = _utc_naive(df["window_start"])
            wide = df.pivot_table(index="window_start", columns="cdn", values="avg_bitrate_kbps")
            st.line_chart(wide)
        else:
            st.info("Sem dados de bitrate ainda (suba o Silver).")

    _silver_panels()

# ====================== ABA GOLD (dbt, batch auditável) ======================
with tab_gold:
    st.caption(
        "Batch layer (Gold/dbt) — CCV, impacto comercial e QoE por rede, auditável e "
        "idempotente."
    )

    # --- CCV por região ---
    st.subheader("CCV por região (pico)")
    if ccv:
        df = pd.DataFrame(ccv).set_index("region")
        st.bar_chart(df["peak_ccv"])
    else:
        st.info("Sem dados de CCV (rode o Gold).")

    # --- Impacto comercial por marker SCTE-35 ---
    st.subheader("Impacto comercial por marker SCTE-35")
    if ads:
        st.dataframe(pd.DataFrame(ads), use_container_width=True, hide_index=True)
    else:
        st.info("Sem dados de ad impact (rode o Gold).")

    # --- Criativos por break: qual ad/empresa rodou ---
    st.subheader("Criativos exibidos por break (anunciante / criativo)")
    if ad_creatives:
        st.caption(
            "Detalhe do break: `advertiser_name` = empresa, `creative_id` = criativo "
            "(sem nome humano na fonte). Receita estimada = CPM × impressões / 1000."
        )
        st.dataframe(pd.DataFrame(ad_creatives), use_container_width=True, hide_index=True)
    else:
        st.info("Sem detalhe de criativos (rode o Gold após adicionar gold_ad_creatives).")

    # --- QoE por network_type (dimensão do schema v2 — F6) ---
    st.subheader("QoE por tipo de rede (network_type — schema v2)")
    if net_qoe:
        st.caption(
            "Dimensão adicionada no schema **v2** (BACKWARD-compatible). Eventos v1 (sem o "
            "campo) entram como `unknown` por default-fill no staging do Gold — a evolução "
            "entrega valor analítico sem quebrar o histórico v1."
        )
        st.dataframe(pd.DataFrame(net_qoe), use_container_width=True, hide_index=True)
    else:
        st.info("Sem coluna network_type na Gold ainda (rode o Gold após o schema v2).")
