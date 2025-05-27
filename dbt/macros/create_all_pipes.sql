{% macro create_all_pipes_from_config() %}
  {% set pipe_config = var('pipe_configs') %}

  {% for pipe in pipe_config %}
    {% set pipe_name = pipe.name %}
    {% set table = pipe.table %}
    {% set stage = pipe.stage %}
    {% set pattern = pipe.pattern %}
    {% set file_format = pipe.file_format %}

    {% set pipe_exists_query %}
      select count(*) from information_schema.pipes
      where pipe_name = upper('{{ pipe_name }}')
        and table_schema = '{{ target.schema }}'
        and table_catalog = '{{ target.database }}'
    {% endset %}

    {% set result = run_query(pipe_exists_query) %}
    {% set exists = result.columns[0].values()[0] %}

    {% if exists == 0 %}
      {{ log("Creating pipe " ~ pipe_name, info=True) }}

      create or replace pipe {{ target.database }}.{{ target.schema }}.{{ pipe_name }} as
      copy into {{ target.database }}.{{ target.schema }}.{{ table }} (
        {{ pipe.columns | join(', ') }},
        FILE_NAME,
        LOAD_TIME
      )
      from (
        select {{ range(1, pipe.columns|length + 1)|map('string')|map('regex_replace', '^(.*)$', 't.$\\1') | join(', ') }},
               metadata$filename,
               current_timestamp()
        from @{{ target.database }}.{{ target.schema }}.{{ stage }} t
      )
      pattern = '{{ pattern }}'
      file_format = ({{ file_format | items | map('join', '=') | join(' ') }});
    {% else %}
      {{ log("Pipe " ~ pipe_name ~ " already exists, skipping.", info=True) }}
    {% endif %}

  {% endfor %}
{% endmacro %}
