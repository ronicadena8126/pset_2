select
    cast(ID as integer)                         as customer_id,
    cast(DEFAULT_PAYMENT_NEXT_MONTH as integer) as default_next_month,
    cast(LABEL_MONTH || '-01' as date)          as label_month,
    cast(_INGESTED_AT as timestamp)             as ingested_at
from {{ source('raw', 'DEFAULT_LABELS') }}
