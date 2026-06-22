-- Detalhe de criativos por break SCTE-35: qual anunciante/criativo rodou em cada marker.
-- Grão = (event_id_scte, advertiser, creative). Complementa gold_ad_impact (que é por marker)
-- respondendo "qual ad rodou, de qual empresa". Receita = CPM * impressões / 1000 (mesma
-- fórmula do gold_ad_impact, agora atribuída ao criativo).
with breaks as (
    select
        marker_id,
        event_id_scte,
        break_type
    from {{ ref('stg_scte35') }}
    where break_type in ('commercial', 'blackout')
),
decisions as (
    select
        event_id_scte,
        advertiser_id,
        advertiser_name,
        creative_id,
        max(creative_duration_s) as creative_duration_s,
        count(*) as ad_impressions,
        sum(price_cpm_brl) as sum_cpm_brl
    from {{ ref('stg_ad_decisions') }}
    group by 1, 2, 3, 4
)
select
    d.event_id_scte,
    b.break_type,
    d.advertiser_id,
    d.advertiser_name,
    d.creative_id,
    d.creative_duration_s,
    d.ad_impressions,
    round(d.sum_cpm_brl, 2) as sum_cpm_brl,
    round(d.sum_cpm_brl * d.ad_impressions / 1000.0, 2) as est_revenue_brl
from decisions d
left join breaks b on b.event_id_scte = d.event_id_scte
order by est_revenue_brl desc
