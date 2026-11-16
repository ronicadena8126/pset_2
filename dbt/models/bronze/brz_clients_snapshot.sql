-- Bronze: copia 1:1 de RAW con tipos y nombres estandarizados. Sin reglas de negocio.
select
    cast(ID as integer)                      as customer_id,
    cast(LIMIT_BAL as numeric(14,2))         as limit_bal,
    cast(SEX as integer)                     as sex_code,
    cast(EDUCATION as integer)               as education_code,
    cast(MARRIAGE as integer)                as marriage_code,
    cast(AGE as integer)                     as age,
    cast(SNAPSHOT_MONTH || '-01' as date)    as snapshot_month,
    cast(_INGESTED_AT as timestamp)          as ingested_at,
    _EXECUTION_ID                            as execution_id
from {{ source('raw', 'CLIENTS_SNAPSHOT') }}
