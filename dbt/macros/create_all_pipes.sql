{% macro create_all_pipes_from_config() %}
    {% set pipes = var('pipe_configs', []) %}

    {% for pipe in pipes %}
        create or replace pipe {{ pipe.name }} as
        copy into {{ pipe.table }}
        from @{{ pipe.stage }}
        pattern = '{{ pipe.pattern }}'
        file_format = (type = csv field_optionally_enclosed_by='"' skip_header=1);
    {% endfor %}
{% endmacro %}
