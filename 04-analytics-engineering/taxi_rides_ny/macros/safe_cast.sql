
{% macro safe_cast(column, data_type) %}
    {% if target.type == 'bigquery' %}
        safe_cast({{ column }} as {{ data_type }})
    {% else %}
        cast({{ column }} as {{ data_type }})
    {% endif %}
{% endmacro %}

{% macro numeric_type() %}
    {% if target.type == 'bigquery' %}
        NUMERIC
    {% else %}
        decimal(18,3)
    {% endif %}
{% endmacro %}
