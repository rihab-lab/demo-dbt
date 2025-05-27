{{ config(
    materialized = "incremental",
    schema = "SILVER_LAYER",
    unique_key = "PRICINGBENCHMARKPRCINTKEY",
    incremental_strategy = "merge",
    on_schema_change = "append_new_columns",
    post_hook = [
      "alter table {{ this }} add primary key (PRICINGBENCHMARKPRCINTKEY)"
    ]
) }}

with source as (
    select *
    from {{ source('BRONZE_LAYER', 'PRC_BENCHMARK_BRZ_STREAM') }}
),

deduplicated as (
    select *,
           row_number() over (
               partition by to_numeric(hash(coalesce(APUKCODE, 'N/A') || '_' || coalesce(ANABENCH2, 'N/A')))
               order by try_to_timestamp(SYS_SOURCE_DATE) desc nulls last, current_timestamp() desc
           ) as row_num
    from source
)

select
    to_numeric(hash(coalesce(APUKCODE, 'N/A') || '_' || coalesce(ANABENCH2, 'N/A'))) as PRICINGBENCHMARKPRCINTKEY,
    replace(coalesce(APUKCODE, 'N/A') || '_' || coalesce(ANABENCH2, 'N/A'), ' ', '_') as PRICINGBENCHMARKPRCKEY,
    APUKCODE,
    ANABENCH2,
    SKUGROUP,
    SYS_SOURCE_DATE,
    current_timestamp() as SYS_DATE_CREATE
from deduplicated
where row_num = 1
