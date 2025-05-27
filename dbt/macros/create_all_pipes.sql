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

        {% set copy_into_columns %}
            ({{ all_columns | join(', ') }})
        {% endset %}

        {% set select_expr %}
            {{ file_columns | map('regex_replace', '^', 't.$') | join(', ') }}, metadata$filename, current_timestamp()
        {% endset %}

        {% set sql %}
            create or replace pipe {{ qualified_pipe_name }} as
            copy into {{ qualified_table }} {{ copy_into_columns }}
            from (
              select {{ select_expr }}
              from @{{ qualified_stage }} t
            )
            pattern = '{{ pipe.pattern }}'
            file_format = (type = csv field_optionally_enclosed_by='"' skip_header=1);
        {% endset %}

        {% do log("Exécution SQL : " ~ sql, info=True) %}
        {% do run_query(sql) %}
        {% do messages.append("PIPE " ~ qualified_pipe_name ~ " créé") %}
    {% endfor %}

    {% do return(messages) %}
{% endmacro %}
