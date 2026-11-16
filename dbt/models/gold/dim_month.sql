-- Dimensión mes: meses de estado de cuenta + mes del target.
with months as (
    select distinct statement_month as month_start from {{ ref('slv_statements') }}
    union
    select distinct label_month from {{ ref('slv_default_labels') }}
)
select
    cast(extract(year from month_start) * 100 + extract(month from month_start) as integer) as month_key,
    month_start,
    cast(extract(year from month_start) as integer)  as year,
    cast(extract(month from month_start) as integer) as month_num,
    case cast(extract(month from month_start) as integer)
        when 1 then 'enero' when 2 then 'febrero' when 3 then 'marzo' when 4 then 'abril'
        when 5 then 'mayo' when 6 then 'junio' when 7 then 'julio' when 8 then 'agosto'
        when 9 then 'septiembre' when 10 then 'octubre' when 11 then 'noviembre' else 'diciembre'
    end                                               as month_name,
    cast(extract(quarter from month_start) as integer) as quarter,
    -- meses de distancia al mes del target (oct-2005): 1 = último mes observado
    cast(
        (cast(substr('{{ var("label_month") }}', 1, 4) as integer) * 12
         + cast(substr('{{ var("label_month") }}', 6, 2) as integer))
        - (extract(year from month_start) * 12 + extract(month from month_start))
    as integer)                                       as months_to_label,
    (month_start = cast('{{ var("label_month") }}-01' as date)) as is_label_month
from months
