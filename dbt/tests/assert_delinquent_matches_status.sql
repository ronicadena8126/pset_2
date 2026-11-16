-- Consistencia: is_delinquent del hecho debe coincidir con el catálogo de estados de pago.
select f.customer_key, f.month_key
from {{ ref('fct_monthly_statement') }} f
join {{ ref('dim_payment_status') }} p on f.payment_status_key = p.payment_status_key
where f.is_delinquent <> p.is_delinquent
