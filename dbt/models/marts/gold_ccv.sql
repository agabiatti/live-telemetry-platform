-- Audiência: CCV = espectadores simultâneos por janela × dimensão.
select
    window_start,
    window_hour,
    geo_region,
    device_type,
    cdn,
    count(distinct session_id) as ccv
from {{ ref('gold_session_window_qoe') }}
group by 1, 2, 3, 4, 5
