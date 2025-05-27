{% macro generate_bronze_merge_model(pipe_table_name, unique_key) %}
  {% set config_dict = (
      var('pipe_configs')
      | selectattr("table", "equalto", pipe_table_name)
      | list
      | first
  ) %}

  {# Ajout des métadonnées si non déjà présentes #}
  {% set required_columns = ['FILE_NAME', 'LOAD_TIME'] %}
  {% set columns = config_dict.columns %}
  {% for col in required_columns %}
    {% if col not in columns %}
      {% do columns.append(col) %}
    {% endif %}
  {% endfor %}

  {{ config(
      materialized='incremental',
      incremental_strategy='merge',
      unique_key=unique_key,
      schema='BRONZE_LAYER',
      database=var('SF_DATABASE'),
      on_schema_change='append_new_columns'
  ) }}

  select
    {{ columns | join(',\n    ') }}
  from {{ var('SF_DATABASE') }}.{{ var('SF_SCHEMA') }}.{{ pipe_table_name }}
  {% if is_incremental() %}
    where (
      select count(*) from {{ this }}
    ) = 0 or LOAD_TIME > (select max(LOAD_TIME) from {{ this }})
  {% endif %}
{% endmacro %}
