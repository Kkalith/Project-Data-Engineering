{% macro clean_string(col) %}
    lower(trim({{ col }}))
{% endmacro %}