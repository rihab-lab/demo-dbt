{{ config(
    materialized = "table",
    database     = "TEST_POC_VISEO_DB",
    schema       = "RAW_LAYER"
) }}

-- CTAS vide pour ne créer que la structure
select
  cast(null as varchar(16777216))    as HouseKey           -- NOT NULL, tu pourras ajouter un test dbt
  ,cast(null as varchar(16777216))   as CampaignCode       -- NOT NULL
  ,cast(null as varchar(16777216))   as CampaignName
  ,cast(null as varchar(16777216))   as CampaignDescription
  ,cast(null as varchar(16777216))   as HistoricalSellInFirstMonth
  ,cast(null as varchar(16777216))   as HistoricalSellInLastMonth
  ,cast(null as varchar(16777216))   as CampaignDate
  ,cast(null as varchar(16777216))   as SYS_SOURCE_DATE
  ,cast(null as varchar(16777216))   as FILE_NAME
  ,cast(current_timestamp() as timestamp_ntz(9)) as LOAD_TIME
where false
