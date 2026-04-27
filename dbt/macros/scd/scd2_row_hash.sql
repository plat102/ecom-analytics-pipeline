{% macro scd2_row_hash(columns) %}
  MD5(CONCAT(
    {% for col in columns %}
      COALESCE({{ col }}, '')
      {%- if not loop.last %}, {% endif %}
    {% endfor %}
  ))
{% endmacro %}
