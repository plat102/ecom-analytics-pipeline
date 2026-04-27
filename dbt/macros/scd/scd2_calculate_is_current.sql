{#
  Calculate is_current flag for SCD Type 2 versioning.

  Logic:
  - Use LEAD() to check if there's a next version
  - If LEAD() IS NULL: This is the latest version -> is_current = TRUE
  - If LEAD() IS NOT NULL: There's a newer version -> is_current = FALSE

  Parameters:
  - timestamp_column: Column containing version start timestamp
  - partition_by_column: Natural key to partition versions (e.g., customer_natural_key)

  Example:
    customer_natural_key  | first_seen_with_this_email | LEAD() result | is_current
    ---------------------|----------------------------|---------------|------------
    user123              | 2019-10-01                 | 2019-10-15    | FALSE (has next version)
    user123              | 2019-10-15                 | NULL          | TRUE (latest version)
#}
{% macro scd2_calculate_is_current(
    timestamp_column,
    partition_by_column
) %}
  CASE
    WHEN LEAD({{ timestamp_column }}) OVER (
      PARTITION BY {{ partition_by_column }}
      ORDER BY {{ timestamp_column }}
    ) IS NULL
    THEN TRUE
    ELSE FALSE
  END
{% endmacro %}
