-- SCD: mantém a versão mais recente por content_id (maior updated_at).
with src as (
    select * from {{ source('bronze', 'src_content_metadata') }}
),
ranked as (
    select *, row_number() over (partition by content_id order by updated_at desc) as rn
    from src
)
select * exclude (rn)
from ranked
where rn = 1
