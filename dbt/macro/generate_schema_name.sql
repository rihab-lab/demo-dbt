{% macro generate_schema_name(custom_schema_name, node) %}
    {% if custom_schema_name is not none %}
        {{ return(custom_schema_name) }}        {# remplace totalement #}
    {% else %}
        {{ return(target.schema) }}             {# sinon, schema par défaut #}
    {% endif %}
{% endmacro %}
