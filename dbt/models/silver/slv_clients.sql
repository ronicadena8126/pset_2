-- Silver: un registro por cliente, atributos limpios.
-- Decisiones (ver docs): RAW guarda un snapshot por mes cargado -> se conserva el más reciente;
-- EDUCATION 0/5/6 y MARRIAGE 0 no están documentados -> se reasignan a "Otros" con bandera.
with ranked as (
    select
        *,
        row_number() over (
            partition by customer_id
            order by snapshot_month desc, ingested_at desc
        ) as rn
    from {{ ref('brz_clients_snapshot') }}
)
select
    customer_id,
    limit_bal,
    sex_code,
    case sex_code when 1 then 'Hombre' when 2 then 'Mujer' end              as sex,
    education_code                                                           as education_code_raw,
    case when education_code in (1, 2, 3, 4) then education_code else 4 end as education_code,
    case
        when education_code = 1 then 'Posgrado'
        when education_code = 2 then 'Universidad'
        when education_code = 3 then 'Secundaria'
        else 'Otros/Desconocido'
    end                                                                      as education,
    (education_code not in (1, 2, 3, 4))                                     as is_education_imputed,
    marriage_code                                                            as marriage_code_raw,
    case when marriage_code in (1, 2, 3) then marriage_code else 3 end       as marriage_code,
    case
        when marriage_code = 1 then 'Casado'
        when marriage_code = 2 then 'Soltero'
        else 'Otro/Desconocido'
    end                                                                      as marital_status,
    (marriage_code not in (1, 2, 3))                                         as is_marriage_imputed,
    age,
    case
        when age < 25 then '21-24'
        when age < 35 then '25-34'
        when age < 45 then '35-44'
        when age < 55 then '45-54'
        else '55+'
    end                                                                      as age_band,
    snapshot_month                                                           as attributes_as_of
from ranked
where rn = 1
