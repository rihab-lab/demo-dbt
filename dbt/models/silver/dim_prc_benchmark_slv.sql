{{ config(
    materialized         = "incremental",
    schema               = "SILVER_LAYER",
    unique_key           = "PRICINGBENCHMARKPRCINTKEY",
    incremental_strategy = "merge",
    on_schema_change     = "append_new_columns"
) }}

{% set bronze_schema = 'BRONZE_LAYER' %}
{% set bronze_table  = 'PRC_BENCHMARK_BRZ' %}
{% set bronze_stream = 'PRC_BENCHMARK_BRZ_STREAM' %}

{%- if not is_incremental() -%}
  -- Premier chargement ou full-refresh : on lit la table Bronze (tout l’historique)
  with source as (
    select *
    from {{ source(bronze_schema, bronze_table) }}
  )
{%- else -%}
  -- Mode incrémental : on lit uniquement le Stream (seulement les nouveaux changements)
  with source as (
    select *
    from {{ source(bronze_schema, bronze_stream) }}
  )
{%- endif %}

, deduplicated as (
  select *,
    row_number() over (
      partition by APUKCODE, ANABENCH2CODE
      order by LOAD_TIME desc
    ) as row_num
  from source
)

select
  to_number(hash(coalesce(APUKCODE, 'N/A') || '_' || coalesce(ANABENCH2CODE, 'N/A'))) as PRICINGBENCHMARKPRCINTKEY,
  replace(coalesce(APUKCODE, 'N/A') || '_' || coalesce(ANABENCH2CODE, 'N/A'), ' ', '_') as PRICINGBENCHMARKPRCKEY,
  APUKCODE,
  ANABENCH2CODE,
  ANABENCH2,
  SKUGROUP,
  SYS_SOURCE_DATE,
  current_timestamp() as SYS_DATE_CREATE
from deduplicated
where row_num = 1

{% if execute and this is not none %}
  {% do add_primary_key_if_not_exists(this, 'PRICINGBENCHMARKPRCINTKEY') %}
{% endif %}
