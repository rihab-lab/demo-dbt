{{ 
  config(
    materialized = "table",
    database     = "TEST_POC_VISEO_DB",
    schema       = "RAW_LAYER",
    post_hook    = [
      "{{ copy_into_raw(
           table_name     = this.identifier,
           prefix_pattern = 'PRC_CAMPAIGN',
           columns        = [
             'HOUSEKEY',
             'CAMPAIGNCODE',
             'CAMPAIGNNAME',
             'CAMPAIGNDESCRIPTION',
             'HISTORICALSELLINFIRSTMONTH',
             'HISTORICALSELLINLASTMONTH',
             'CAMPAIGNDATE',
             'FILE_NAME',
             'SYS_SOURCE_DATE'
           ],
           select_exprs   = [
             't.$1          AS HOUSEKEY',
             't.$2          AS CAMPAIGNCODE',
             't.$3          AS CAMPAIGNNAME',
             't.$4          AS CAMPAIGNDESCRIPTION',
             't.$5          AS HISTORICALSELLINFIRSTMONTH',
             't.$6          AS HISTORICALSELLINLASTMONTH',
             't.$7          AS CAMPAIGNDATE',
             'METADATA$FILENAME   AS FILE_NAME',
             'METADATA$CREATED_ON AS SYS_SOURCE_DATE'
           ]
         ) }}"
    ]
  ) 
}}

select
  cast(null as varchar(16777216)) as HouseKey,
  cast(null as varchar(16777216)) as CampaignCode,
  cast(null as varchar(16777216)) as CampaignName,
  cast(null as varchar(16777216)) as CampaignDescription,
  cast(null as varchar(16777216)) as HistoricalSellInFirstMonth,
  cast(null as varchar(16777216)) as HistoricalSellInLastMonth,
  cast(null as varchar(16777216)) as CampaignDate,
  cast(null as varchar(16777216)) as FILE_NAME,
  cast(null as timestamp_ltz)     as SYS_SOURCE_DATE
where false
