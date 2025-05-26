-- models/raw/prc_benchmark_raw.sql

-- On ne rechargera jamais deux fois un même fichier grâce à unique_key
{{ 
  config(
    materialized = "incremental",
    database     = "TEST_POC_VISEO_DB",
    schema       = "RAW_LAYER",
    unique_key   = "FILE_NAME"
  )
}}

with staged as (
  select
    t.$1::varchar(16777216)               as APUKCODE,
    t.$2::varchar(16777216)               as ANABENCH2CODE,
    t.$3::varchar(16777216)               as ANABENCH2,
    t.$4::varchar(16777216)               as SKUGROUP,
    t.metadata$filename                   as FILE_NAME,
    t.metadata$created_on::timestamp_ltz  as SYS_SOURCE_DATE
  from @{{ this.database }}.{{ this.schema }}.EXTERNAL_AZURE_STAGE as t
  where t.metadata$filename like 'PRC_BENCHMARK_%'
)

select * from staged

{% if is_incremental() %}
  where SYS_SOURCE_DATE > (
    select max(SYS_SOURCE_DATE) from {{ this }}
  )
{% endif %}
