{# Test genérico: falla con las filas donde la expresión de negocio NO se cumple. #}
{% test expression_is_true(model, expression, column_name=None) %}
select * from {{ model }} where not ({{ expression }})
{% endtest %}
