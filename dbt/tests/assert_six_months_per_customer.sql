-- Regla de negocio: el modelo usa 6 meses de historia; todo cliente debe tenerlos completos.
select customer_key, count(*) as months
from {{ ref('fct_monthly_statement') }}
group by customer_key
having count(*) <> 6
