{% macro generate_incremental_surrogate_key(
    order_by_columns,
    target_table=this,
    key_column='id'
) %}
  ROW_NUMBER() OVER (ORDER BY {{ order_by_columns }}) +
    (SELECT COALESCE(MAX({{ key_column }}), 0) FROM {{ target_table }})
{% endmacro %}
