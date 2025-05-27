{% macro create_all_pipes_from_config() %}
    {% set pipes = var('pipe_configs', []) %}
    {% set database = var('SF_DATABASE') %}
    {% set schema = var('SF_SCHEMA') %}

    {% do log("Database utilisé : " ~ database, info=True) %}
    {% do log("Schéma utilisé   : " ~ schema, info=True) %}
    {% do log("Nombre de pipes à créer : " ~ pipes | length, info=True) %}

    {% set messages = [] %}

    {% for pipe in pipes %}
        {% set qualified_pipe_name = database ~ '.' ~ schema ~ '.' ~ pipe.name %}
        {% set qualified_table = database ~ '.' ~ schema ~ '.' ~ pipe.table %}
        {% set qualified_stage = database ~ '.' ~ schema ~ '.' ~ pipe.stage %}

        {% set file_columns = pipe.columns %}
        {% set all_columns = file_columns + ['FILE_NAME', 'LOAD_TIME'] %}

        {% set copy_into_columns = '(' ~ all_columns | join(', ') ~ ')' %}

        {% set select_parts = [] %}
        {% for i in range(1, file_columns | length + 1) %}
            {% do select_parts.append('t.$' ~ i) %}
        {% endfor %}
        {% do select_parts.append('metadata$filename') %}
        {% do select_parts.append('current_timestamp()') %}
        {% set select_expr = select_parts | join(', ') %}

        {% set fmt = pipe.get('file_format', {}) %}
        {% set file_format_config %}
            file_format = (
              type = csv
              {% if fmt.field_delimiter is defined %} field_delimiter = '{{ fmt.field_delimiter }}' {% endif %}
              {% if fmt.skip_header is defined %} skip_header = {{ fmt.skip_header }} {% endif %}
              {% if fmt.field_optionally_enclosed_by is defined %} field_optionally_enclosed_by = '{{ fmt.field_optionally_enclosed_by }}' {% endif %}
            )
        {% endset %}

        {# Vérifie si le pipe existe déjà #}
       {% set check_pipe_query %}
       select count(*) from information_schema.pipes
       where PIPE_NAME = upper('{{ pipe.name }}')
      and TABLE_SCHEMA = upper('{{ schema }}')
      and TABLE_CATALOG = upper('{{ database }}')
{% endset %}
        {% set check_result = run_query(check_pipe_query) %}
        {% set pipe_exists = check_result.columns[0].values()[0] %}

        {% if pipe_exists == 0 %}
            {% set sql %}
                create or replace pipe {{ qualified_pipe_name }} as
                copy into {{ qualified_table }} {{ copy_into_columns }}
                from (
                  select {{ select_expr }}
                  from @{{ qualified_stage }} t
                )
                pattern = '{{ pipe.pattern }}'
                {{ file_format_config }};
            {% endset %}

            {% do log(" Création du pipe : " ~ qualified_pipe_name, info=True) %}
            {% do log("Exécution SQL : " ~ sql, info=True) %}
            {% do run_query(sql) %}
            {% do messages.append("PIPE " ~ qualified_pipe_name ~ " créé") %}
        {% else %}
            {% do log(" Pipe déjà existant : " ~ qualified_pipe_name ~ ", ignoré", info=True) %}
        {% endif %}
    {% endfor %}

    {% do return(messages) %}
{% endmacro %}
