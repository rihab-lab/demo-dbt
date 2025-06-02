{% macro add_primary_key_if_not_exists(model, key_column) %}
    {% set database = model.database %}
    {% set schema = model.schema %}
    {% set table = model.alias %}

    {% set sql %}
        select count(*) as key_exists
        from information_schema.table_constraints
        where constraint_type = 'PRIMARY KEY'
        and table_name = upper('{{ table }}')
        and table_schema = upper('{{ schema }}')
    {% endset %}

    {% set results = run_query(sql) %}
    {% if execute and results %}
        {% set key_exists = results.columns[0].values()[0] %}
        {% if key_exists == 0 %}
            alter table {{ model }} add primary key ({{ key_column }});
        {% else %}
            -- primary key already exists
        {% endif %}
    {% endif %}
{% endmacro %}
