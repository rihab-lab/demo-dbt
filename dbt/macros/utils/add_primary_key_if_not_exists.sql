-- macros/add_primary_key_if_not_exists.sql

{% macro add_primary_key_if_not_exists(model, column_name) %}
  {% if not execute %}
    {% do return(None) %}
  {% endif %}

  {% set database = model.database %}
  {% set schema = model.schema %}
  {% set table = model.table %}

  {% set check_query %}
      SELECT count(*) 
      FROM information_schema.table_constraints 
      WHERE table_schema = '{{ schema }}' 
        AND table_name = '{{ table }}'
        AND constraint_type = 'PRIMARY KEY'
  {% endset %}

  {% set result = run_query(check_query) %}
  {% if result and result.columns[0].name == 'COUNT' and result.rows[0][0] == 0 %}
    alter table {{ model }} add primary key ({{ column_name }});
  {% else %}
    -- clé déjà présente
  {% endif %}
{% endmacro %}
