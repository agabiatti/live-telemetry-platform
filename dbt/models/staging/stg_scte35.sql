select
    marker_id,
    channel,
    splice_command,
    event_id_scte,
    out_of_network,
    pts_time,
    wallclock,
    duration_s,
    break_type
from {{ source('bronze', 'src_scte35_markers') }}
