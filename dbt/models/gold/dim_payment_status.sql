-- Dimensión estado de pago (catálogo de la fuente + aclaraciones del autor del dataset).
select
    pay_status          as payment_status_key,
    pay_status,
    status_group,
    description,
    is_delinquent,
    delay_bucket
from (
    values
        (-2, 'SIN_CONSUMO', 'Sin consumo en el período',               false, 'Al día'),
        (-1, 'PAGO_TOTAL',  'Pagó el total del estado de cuenta',      false, 'Al día'),
        ( 0, 'PAGO_MINIMO', 'Pagó al menos el mínimo (crédito rotativo)', false, 'Al día'),
        ( 1, 'ATRASO',      'Atraso de 1 mes',                         true,  '1 mes'),
        ( 2, 'ATRASO',      'Atraso de 2 meses',                       true,  '2 meses'),
        ( 3, 'ATRASO',      'Atraso de 3 meses',                       true,  '3+ meses'),
        ( 4, 'ATRASO',      'Atraso de 4 meses',                       true,  '3+ meses'),
        ( 5, 'ATRASO',      'Atraso de 5 meses',                       true,  '3+ meses'),
        ( 6, 'ATRASO',      'Atraso de 6 meses',                       true,  '3+ meses'),
        ( 7, 'ATRASO',      'Atraso de 7 meses',                       true,  '3+ meses'),
        ( 8, 'ATRASO',      'Atraso de 8 meses',                       true,  '3+ meses'),
        ( 9, 'ATRASO',      'Atraso de 9 meses o más',                 true,  '3+ meses')
) as t (pay_status, status_group, description, is_delinquent, delay_bucket)
