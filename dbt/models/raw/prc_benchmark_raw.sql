{{ 
  config(
    materialized = "incremental",
    database     = "TEST_POC_VISEO_DB",
    schema       = "RAW_LAYER",
    unique_key   = "FILE_NAME"   -- on ne rechargera jamais deux fois un même fichier
  )
}}

with staged as (
  select
    $1::varchar(16777216)                         as APUKCode,
    $2::varchar(16777216)                         as Anabench2Code,
    $3::varchar(16777216)                         as Anabench2,
    $4::varchar(16777216)                         as SKUGroup,
    metadata$filename                             as FILE_NAME,
    metadata$created_on::timestamp_ltz            as SYS_SOURCE_DATE
  from @{{ this.database }}.{{ this.schema }}.EXTERNAL_AZURE_STAGE
  where metadata$filename like 'PRC_BENCHMARK_%'
)

select * from staged

{% if is_incremental() %}
  -- sur les runs suivants, on ne prend que les fichiers plus récents que le max déjà chargé
  where SYS_SOURCE_DATE > (
    select max(SYS_SOURCE_DATE) from {{ this }}
  )
{% endif %}
