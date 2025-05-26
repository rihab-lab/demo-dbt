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
    $1::varchar(16777216)                         as HouseKey,
    $2::varchar(16777216)                         as CampaignCode,
    $3::varchar(16777216)                         as CampaignName,
    $4::varchar(16777216)                         as CampaignDescription,
    $5::varchar(16777216)                         as HistoricalSellInFirstMonth,
    $6::varchar(16777216)                         as HistoricalSellInLastMonth,
    $7::varchar(16777216)                         as CampaignDate,
    metadata$filename                             as FILE_NAME,
    metadata$created_on::timestamp_ltz            as SYS_SOURCE_DATE
  from @{{ this.database }}.{{ this.schema }}.EXTERNAL_AZURE_STAGE
  where metadata$filename like 'PRC_CAMPAIGN_%'
)

select * from staged

{% if is_incremental() %}
  where SYS_SOURCE_DATE > (
    select max(SYS_SOURCE_DATE) from {{ this }}
  )
{% endif %}
