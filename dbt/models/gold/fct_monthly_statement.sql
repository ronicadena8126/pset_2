-- Hecho principal. GRANO: un estado de cuenta por cliente y por mes (abr-sep 2005).
select
    d.customer_key,
    cast(extract(year from s.statement_month) * 100 + extract(month from s.statement_month) as integer) as month_key,
    s.pay_status                    as payment_status_key,
    s.customer_id,                  -- dimensión degenerada, útil para trazabilidad
    s.bill_amt,
    s.pay_amt,
    s.prev_bill_amt,
    s.months_delay,
    s.is_delinquent,
    s.utilization_ratio,
    s.payment_ratio,
    s.has_credit_balance,
    s.is_overlimit,
    s.is_extreme_overlimit,
    s.flag_delay_without_balance,
    s.flag_no_use_with_balance
from {{ ref('slv_statements') }} as s
inner join {{ ref('dim_customer') }} as d
    on s.customer_id = d.customer_id
