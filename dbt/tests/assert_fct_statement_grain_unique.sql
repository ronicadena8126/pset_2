-- El grano de fct_monthly_statement es cliente x mes: no puede haber dos filas iguales.
select customer_key, month_key, count(*) as n
from {{ ref('fct_monthly_statement') }}
group by customer_key, month_key
having count(*) > 1
