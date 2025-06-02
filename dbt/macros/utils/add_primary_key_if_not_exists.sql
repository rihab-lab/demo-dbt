{% macro add_primary_key_if_not_exists(relation, column) %}
    {% set relation_id = relation.database ~ '.' ~ relation.schema ~ '.' ~ relation.identifier %}
    {% set check_pk_query %}
        select count(*) as count
        from information_schema.table_constraints
        where constraint_type = 'PRIMARY KEY'
        and table_name = '{{ relation.identifier | upper }}'
        and table_schema = '{{ relation.schema | upper }}'
    {% endset %}

    {% set result = run_query(check_pk_query) %}
    {% if result.columns[0].values()[0] == 0 %}
        alter table {{ relation_id }} add primary key ({{ column }})
    {% else %}
        -- primary key already exists, skipping
    {% endif %}
{% endmacro %}
