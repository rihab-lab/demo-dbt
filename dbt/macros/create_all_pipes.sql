{% macro create_all_pipes_from_config() %}
    {% set pipes = var('pipe_configs', []) %}
    {% do log("Nombre de pipes à créer : " ~ pipes | length, info=True) %}

    {% set messages = [] %}

    {% for pipe in pipes %}
        {% set sql %}
            create or replace pipe {{ pipe.name }} as
            copy into {{ pipe.table }}
            from @{{ pipe.stage }}
            pattern = '{{ pipe.pattern }}'
            file_format = (type = csv field_optionally_enclosed_by='"' skip_header=1);
        {% endset %}

        {% do log("Exécution SQL : " ~ sql, info=True) %}
        {% do run_query(sql) %}
        {% do messages.append("PIPE " ~ pipe.name ~ " créé") %}
    {% endfor %}

    {% do return(messages) %}
{% endmacro %}
