-- Anti-fuga: el mes del target debe ser estrictamente posterior a todo estado de cuenta usado.
select f.customer_key
from {{ ref('fct_default_outcome') }} f
join {{ ref('fct_monthly_statement') }} s on f.customer_key = s.customer_key
where s.month_key >= f.label_month_key
