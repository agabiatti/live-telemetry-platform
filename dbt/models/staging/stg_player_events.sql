-- Staging player events: dedupe GLOBAL por event_id (regra estrita do Gold) e
-- propagação do schema evolution (network_type default 'unknown' → F6).
with src as (
    select * from {{ source('bronze', 'src_player_events') }}
),
deduped as (
    select
        *,
        row_number() over (partition by event_id order by _ingested_at, _kafka_offset) as rn
    from src
)
select
    event_id,
    session_id,
    user_id,
    event_time,
    event_type,
    content_id,
    is_live,
    device_type,
    geo_region,
    geo_state,
    geo_city,
    cdn,
    bitrate_kbps,
    buffer_length_ms,
    playhead_position_s,
    error_code,
    coalesce(network_type, 'unknown') as network_type,  -- F6: default aplicado no Gold
    date_trunc('minute', event_time) as window_start
from deduped
where rn = 1
