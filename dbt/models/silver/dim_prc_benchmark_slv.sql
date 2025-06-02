{{ config(
    materialized = "incremental",
    schema = "SILVER_LAYER",
    unique_key = "PRICINGBENCHMARKPRCINTKEY",
    incremental_strategy = "merge",
    on_schema_change = "append_new_columns",
    post_hook = [
        "{{ add_primary_key_if_not_exists(this, 'PRICINGBENCHMARKPRCINTKEY') }}"
    ]
) }}

with source as (
    select *
    from {{ source('BRONZE_LAYER', 'PRC_BENCHMARK_BRZ_STREAM') }}
),

deduplicated as (
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
