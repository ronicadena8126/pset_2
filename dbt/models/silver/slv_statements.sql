-- Silver: estado de cuenta limpio, grano cliente x mes.
-- PAY_STATUS se conserva (no se imputa): -2, -1 y 0 tienen significado propio
-- (sin consumo / pago total / pago mínimo). Se derivan variables robustas a esa codificación.
-- Saldos negativos y sobregiros son válidos en negocio: se conservan y se marcan.
with dedup as (
    select
        *,
        row_number() over (
            partition by customer_id, statement_month
            order by ingested_at desc
        ) as rn
    from {{ ref('brz_statements') }}
),

stm as (
    select * from dedup where rn = 1
),

enriched as (
    select
        stm.customer_id,
        stm.statement_month,
        stm.pay_status,
        case
            when stm.pay_status = -2 then 'SIN_CONSUMO'
            when stm.pay_status = -1 then 'PAGO_TOTAL'
            when stm.pay_status = 0  then 'PAGO_MINIMO'
            else 'ATRASO'
        end                                                     as pay_status_group,
        case when stm.pay_status >= 1 then stm.pay_status else 0 end as months_delay,
        (stm.pay_status >= 1)                                   as is_delinquent,
        stm.bill_amt,
        stm.pay_amt,
        lag(stm.bill_amt) over (
            partition by stm.customer_id order by stm.statement_month
        )                                                       as prev_bill_amt,
        c.limit_bal,
        stm.ingested_at
    from stm
    inner join {{ ref('slv_clients') }} as c
        on stm.customer_id = c.customer_id
)

select
    customer_id,
    statement_month,
    pay_status,
    pay_status_group,
    months_delay,
    is_delinquent,
    bill_amt,
    pay_amt,
    prev_bill_amt,
    case when limit_bal > 0 then bill_amt / limit_bal end                 as utilization_ratio,
    case when prev_bill_amt > 0 then pay_amt / prev_bill_amt end          as payment_ratio,
    (bill_amt < 0)                                                        as has_credit_balance,
    (bill_amt > limit_bal)                                                as is_overlimit,
    (bill_amt > 2 * limit_bal)                                            as is_extreme_overlimit,
    (pay_status >= 1 and bill_amt <= 0)                                   as flag_delay_without_balance,
    (pay_status = -2 and bill_amt > 0)                                    as flag_no_use_with_balance,
    ingested_at
from enriched
