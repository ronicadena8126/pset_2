-- Dimensión cliente (SCD tipo 1: atributos vigentes al último snapshot).
with activity as (
    select
        customer_id,
        sum(case when bill_amt <> 0 or pay_amt <> 0 then 1 else 0 end) as active_months,
        sum(case when has_credit_balance then 1 else 0 end)             as months_with_credit_balance
    from {{ ref('slv_statements') }}
    group by customer_id
)
select
    md5(cast(c.customer_id as varchar))     as customer_key,
    c.customer_id,
    c.limit_bal,
    c.sex,                     -- solo para auditoría de equidad; NO entra a la OBT
    c.marital_status,          -- idem
    c.education_code,
    c.education,
    c.is_education_imputed,
    c.is_marriage_imputed,
    c.age,
    c.age_band,
    (coalesce(a.active_months, 0) = 0)      as is_inactive_6m,
    coalesce(a.months_with_credit_balance, 0) as months_with_credit_balance,
    c.attributes_as_of
from {{ ref('slv_clients') }} as c
left join activity as a
    on c.customer_id = a.customer_id
