{% macro generate_bronze_merge_model(pipe_table_name, unique_key) %}
    {% set config_dict = var('pipe_configs')
        | selectattr("table", "equalto", pipe_table_name)
        | list
        | first %}

    {{ config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = unique_key,
        schema = 'BRONZE_LAYER',
        database = var('SF_DATABASE'),
        post_hook = [
            "alter table {{ this }} add primary key (" ~ unique_key | join(', ') ~ ")"
        ],
        on_schema_change = 'append_new_columns'
    ) }}

    select
        {{ config_dict.columns | join(',\n        ') }}
    from {{ var('SF_DATABASE') }}.{{ var('SF_SCHEMA') }}.{{ config_dict.table }}
    {% if is_incremental() %}
      where LOAD_TIME > (select max(LOAD_TIME) from {{ this }})
    {% endif %}
{% endmacro %}
