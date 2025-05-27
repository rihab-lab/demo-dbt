{% macro create_all_pipes_from_config() %}
    {% set config_path = 'pipes/pipes.yml' %}
    {% set pipe_config = fromyaml(load_file(config_path)) %}

    {% for pipe in pipe_config.pipes %}
        create or replace pipe {{ pipe.name }} as
        copy into {{ pipe.table }}
        from @{{ pipe.stage }}
        pattern = '{{ pipe.pattern }}'
        file_format = (type = csv field_optionally_enclosed_by='"' skip_header=1);
    {% endfor %}
{% endmacro %}
