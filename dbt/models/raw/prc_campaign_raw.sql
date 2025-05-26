-- models/raw/prc_campaign_raw.sql

-- Unique key pour ne jamais recharger deux fois le même fichier
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
    t.$1::varchar(16777216)               as HOUSEKEY,
    t.$2::varchar(16777216)               as CAMPAIGNCODE,
    t.$3::varchar(16777216)               as CAMPAIGNNAME,
    t.$4::varchar(16777216)               as CAMPAIGNDESCRIPTION,
    t.$5::varchar(16777216)               as HISTORICALSELLINFIRSTMONTH,
    t.$6::varchar(16777216)               as HISTORICALSELLINLASTMONTH,
    t.$7::varchar(16777216)               as CAMPAIGNDATE,
    t.metadata$filename                   as FILE_NAME,
    t.metadata$created_on::timestamp_ltz  as SYS_SOURCE_DATE
  from @{{ this.database }}.{{ this.schema }}.EXTERNAL_AZURE_STAGE as t
  where t.metadata$filename like 'PRC_CAMPAIGN_%'
)

select * from staged

{% if is_incremental() %}
  where SYS_SOURCE_DATE > (
    select max(SYS_SOURCE_DATE) from {{ this }}
  )
{% endif %}
