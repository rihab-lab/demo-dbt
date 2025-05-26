{{ 
  config(
    materialized = "table",
    database     = "TEST_POC_VISEO_DB",
    schema       = "RAW_LAYER",
    post_hook    = [
      -- appelle ta macro de copy_with_metadata
      "{{ copy_into_raw(
           table_name     = this.identifier,
           prefix_pattern = 'PRC_CAMPAIGN'
         ) }}"
    ]
  ) 
}}

select
  cast(null as varchar(16777216))    as HouseKey           
  ,cast(null as varchar(16777216))   as CampaignCode       
  ,cast(null as varchar(16777216))   as CampaignName
  ,cast(null as varchar(16777216))   as CampaignDescription
  ,cast(null as varchar(16777216))   as HistoricalSellInFirstMonth
  ,cast(null as varchar(16777216))   as HistoricalSellInLastMonth
  ,cast(null as varchar(16777216))   as CampaignDate
  ,cast(null as varchar(16777216))   as SYS_SOURCE_DATE
  ,cast(null as varchar(16777216))   as FILE_NAME
  ,cast(current_timestamp() as timestamp_ntz(9)) as LOAD_TIME
where false
