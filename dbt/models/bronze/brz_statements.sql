select
    cast(ID as integer)                      as customer_id,
    cast(STATEMENT_MONTH || '-01' as date)   as statement_month,
    cast(PAY_STATUS as integer)              as pay_status,
    cast(BILL_AMT as numeric(14,2))          as bill_amt,
    cast(PAY_AMT as numeric(14,2))           as pay_amt,
    SOURCE_COLUMNS                           as source_columns,
    cast(_INGESTED_AT as timestamp)          as ingested_at,
    _EXECUTION_ID                            as execution_id
from {{ source('raw', 'STATEMENTS') }}
