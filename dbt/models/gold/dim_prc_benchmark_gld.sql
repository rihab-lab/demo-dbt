{{ 
  config(
    materialized         = "incremental",
    schema               = "GOLD_LAYER",
    unique_key           = "PrcPcsBenchmarkIntKey",
    incremental_strategy = "merge",
    on_schema_change     = "append_new_columns"
  ) 
}}



with source as (
  select
    PRICINGBENCHMARKPRCINTKEY              as PrcPcsBenchmarkIntKey,
    0                                       as PrcPcsGenericProductIntKey,
    APUKCODE,
    current_timestamp()                     as SYS_DATE_CREATE,
    current_timestamp()                     as SYS_DATE_UPDATE,
    row_number() over (
      partition by PRICINGBENCHMARKPRCINTKEY
      order by SYS_DATE_UPDATE desc
    )                                       as row_num
  from {{ source('SILVER_LAYER', 'dim_prc_benchmark_slv_stream') }}
  where PRICINGBENCHMARKPRCINTKEY is not null
    and APUKCODE                  is not null
),

deduplicated as (
  select *
  from source
  where row_num = 1
)

select
  PrcPcsBenchmarkIntKey,
  PrcPcsGenericProductIntKey,
  APUKCODE,
  SYS_DATE_CREATE,
  SYS_DATE_UPDATE
from deduplicated


{% if execute and this is not none %}
  {% do add_primary_key_if_not_exists(this, 'PrcPcsBenchmarkIntKey') %}
{% endif %}