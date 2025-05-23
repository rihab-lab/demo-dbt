-- dbt/models/gold/dim_prc_campaign_gld.sql

-- On ajoute la clé primaire en post-hook (métadonnée Snowflake)
{{ config(
    materialized = "table",
    schema       = "GOLD_LAYER",
    post_hook    = [
      "alter table {{ this }} add primary key (PrcPcsCampaignIntKey)"
    ]
) }}

select
  cast(null  as number)      as PrcPcsCampaignIntKey,       -- PK NOT NULL
  cast(null  as varchar)     as HouseKey,                   -- NOT NULL
  cast(null  as varchar)     as CampaignCode,               -- NOT NULL
  cast(null  as varchar)     as CampaignName,
  cast(null  as varchar)     as CampaignDescription,
  cast(null  as varchar)     as HistoricalSellInFirstMonth,
  cast(null  as varchar)     as HistoricalSellInLastMonth,
  cast(null  as date)        as CampaignDate,
  cast(current_timestamp() as timestamp_ltz) as SYS_DATE_CREATE,  -- NOT NULL
  cast(current_timestamp() as timestamp_ltz) as SYS_DATE_UPDATE   -- NOT NULL
where false
