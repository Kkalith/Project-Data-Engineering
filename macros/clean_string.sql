{% macro clean_string(col) %}
    concat(upper(left(trim({{ col }}), 1)), lower(substr(trim({{ col }}), 2)))
{% endmacro %}