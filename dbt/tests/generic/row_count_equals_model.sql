{# Falla si el modelo no tiene exactamente el mismo número de filas que compare_model
   (se usa para garantizar que la limpieza no pierde ni duplica clientes). #}
{% test row_count_equals_model(model, compare_model, compare_expression="count(*)") %}
with a as (select count(*) as n from {{ model }}),
     b as (select {{ compare_expression }} as n from {{ compare_model }})
select a.n as model_rows, b.n as expected_rows from a cross join b where a.n <> b.n
{% endtest %}
