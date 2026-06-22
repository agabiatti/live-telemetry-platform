-- Reconciliação ad↔audiência por marker SCTE-35.
-- ad_decisions NÃO casa por user_id (ad server não vê o user da plataforma); o elo é
-- marker_id / event_id_scte + janela temporal de sessões ativas durante o break.
with breaks as (
    select
        marker_id,
        event_id_scte,
        break_type,
        wallclock as break_start,
        wallclock + (duration_s * interval 1 second) as break_end
    from {{ ref('stg_scte35') }}
    where break_type in ('commercial', 'blackout')
),
active as (
    select
        b.marker_id,
        b.event_id_scte,
        b.break_type,
        count(distinct q.session_id) as active_sessions
    from breaks b
    join {{ ref('gold_session_window_qoe') }} q
        on q.window_start < b.break_end
       and q.window_end > b.break_start
    group by 1, 2, 3
),
ads as (
    select
        event_id_scte,
        count(*) as ad_impressions,
        sum(price_cpm_brl) as sum_cpm_brl
    from {{ ref('stg_ad_decisions') }}
    group by 1
)
select
    a.marker_id,
    a.event_id_scte,
    a.break_type,
    a.active_sessions,
    coalesce(ad.ad_impressions, 0) as ad_impressions,
    round(coalesce(ad.sum_cpm_brl, 0), 2) as sum_cpm_brl,
    round(coalesce(ad.sum_cpm_brl, 0) * coalesce(ad.ad_impressions, 0) / 1000.0, 2) as est_revenue_brl
from active a
left join ads ad on ad.event_id_scte = a.event_id_scte
