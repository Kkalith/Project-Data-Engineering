{% macro safe_cast(column, type) %}
    try_cast({{ column }} as {{ type }})
{% endmacro %}