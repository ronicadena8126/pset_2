-- Regla de negocio: la tasa de impago debe estar en un rango plausible (15%-30%).
-- Si se sale, probablemente hubo un error de carga o de codificación del target.
select avg(cast(default_next_month as double)) as default_rate
from {{ ref('fct_default_outcome') }}
having avg(cast(default_next_month as double)) not between 0.15 and 0.30
