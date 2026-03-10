{% macro dev_limit(n=1000) %}
    {% if target.name == 'dev' %}
        limit {{ n }}
    {% endif %}
{% endmacro %}
