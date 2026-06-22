select
    decision_id,
    marker_id,
    event_id_scte,
    channel,
    user_id_anon,
    slot_index,
    creative_id,
    creative_duration_s,
    advertiser_id,
    advertiser_name,
    price_cpm_brl,
    served_ts,
    ssai_provider
from {{ source('bronze', 'src_ad_decisions') }}
