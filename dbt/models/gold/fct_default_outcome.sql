-- Hecho del resultado. GRANO: un cliente por mes de observación del target.
-- observation_month_key = último mes que el modelo puede ver (sep-2005);
-- label_month_key = mes en el que se observa el impago (oct-2005). Evita fuga de datos.
select
    d.customer_key,
    cast(extract(year from l.label_month) * 100 + extract(month from l.label_month) as integer) as label_month_key,
    cast(replace('{{ var("observation_month") }}', '-', '') as integer)                         as observation_month_key,
    l.customer_id,
    l.default_next_month
from {{ ref('slv_default_labels') }} as l
inner join {{ ref('dim_customer') }} as d
    on l.customer_id = d.customer_id
