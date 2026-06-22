-- Gold QoE por (session_id, janela 1min) — ESPELHA common/metrics.py (fonte única).
-- Regras estritas: dedupe global (no staging), cobertura completa, sessões fechadas.
with ev as (
    select * from {{ ref('stg_player_events') }}
),
buf as (
    select
        session_id, window_start, event_time, event_type,
        lead(event_time) over (partition by session_id order by event_time) as nxt_t,
        lead(event_type) over (partition by session_id order by event_time) as nxt_ty
    from ev
    where event_type in ('buffer_start', 'buffer_end')
),
rebuf as (
    select
        session_id, window_start,
        sum(case when event_type = 'buffer_start' and nxt_ty = 'buffer_end'
                 then epoch_ms(nxt_t) - epoch_ms(event_time) else 0 end) as rebuffer_ms
    from buf
    group by 1, 2
),
-- Sessão fechada (regra estrita do batch): a sessão atingiu um terminal (video_end ou error)
-- em ALGUM ponto do dia. No batch isso é determinístico (vê a sessão inteira); no Silver é
-- só aproximado por timeout/gap. Flag por SESSÃO, propagada a todas as janelas dela.
session_state as (
    select session_id, bool_or(event_type in ('video_end', 'error')) as session_complete
    from ev
    group by 1
),
agg as (
    select
        session_id,
        window_start,
        any_value(geo_region) as geo_region,
        any_value(device_type) as device_type,
        any_value(cdn) as cdn,
        any_value(network_type) as network_type,  -- F6: dimensão do schema v2 (default 'unknown' no staging)
        any_value(content_id) as content_id,      -- chave do join com content_metadata
        count(*) as events,
        sum(case when event_type = 'error' then 1 else 0 end) as error_count,
        avg(case when event_type in ('heartbeat', 'video_start', 'bitrate_switch')
                 then bitrate_kbps end) as avg_bitrate_kbps,
        sum(case when event_type = 'buffer_start' then 1 else 0 end) as buffer_start_count,
        bool_or(event_type = 'video_start') as had_video_start,
        bool_or(event_type in ('video_end', 'error')) as had_terminal,
        min(event_time) as min_t,
        max(event_time) as max_t
    from ev
    group by 1, 2
)
select
    a.session_id,
    a.window_start,
    a.window_start + interval 1 minute as window_end,
    strftime(a.window_start, '%Y-%m-%dT%H') as window_hour,
    a.session_id || '|' || strftime(a.window_start, '%Y-%m-%dT%H:%M') as window_key,
    a.geo_region,
    a.device_type,
    a.cdn,
    a.network_type,
    a.content_id,
    c.title as content_title,
    c.genre as content_genre,
    c.is_premium as content_is_premium,  -- join SCD content_metadata (última versão por content_id)
    s.session_complete,                   -- regra estrita: sessão atingiu terminal no dia
    a.events,
    coalesce(r.rebuffer_ms, 0) as rebuffer_ms,
    greatest(0, (epoch_ms(a.max_t) - epoch_ms(a.min_t)) - coalesce(r.rebuffer_ms, 0)) as watch_ms,
    case
        when (coalesce(r.rebuffer_ms, 0)
              + greatest(0, (epoch_ms(a.max_t) - epoch_ms(a.min_t)) - coalesce(r.rebuffer_ms, 0))) > 0
        then coalesce(r.rebuffer_ms, 0)::double
             / (coalesce(r.rebuffer_ms, 0)
                + greatest(0, (epoch_ms(a.max_t) - epoch_ms(a.min_t)) - coalesce(r.rebuffer_ms, 0)))
        else 0
    end as rebuffering_ratio,
    coalesce(a.avg_bitrate_kbps, 0) as avg_bitrate_kbps,
    a.error_count,
    a.error_count::double / nullif(a.events, 0) as error_rate,
    a.buffer_start_count,
    a.had_video_start,
    (a.had_terminal and not a.had_video_start) as exit_before_video_start
from agg a
left join rebuf r using (session_id, window_start)
left join session_state s on s.session_id = a.session_id
left join {{ ref('stg_content') }} c on c.content_id = a.content_id
