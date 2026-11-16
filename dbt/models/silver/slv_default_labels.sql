with ranked as (
    select
        *,
        row_number() over (partition by customer_id order by ingested_at desc) as rn
    from {{ ref('brz_default_labels') }}
)
select customer_id, default_next_month, label_month
from ranked
where rn = 1
