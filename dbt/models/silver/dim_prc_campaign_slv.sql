-- dbt/models/silver/dim_prc_campaign_slv.sql

-- On ajoute la clé primaire en post-hook (métadonnée Snowflake)
{{ config(
    materialized = "table",
    schema       = "SILVER_LAYER",
    post_hook    = [
      "alter table {{ this }} add primary key (PricingCampaignPrcIntKey)"
    ]
) }}

select
  cast(null as number)               as PricingCampaignPrcIntKey,  -- NOT NULL
  cast(null as varchar(16777216))    as PricingCampaignPrcKey,     -- NOT NULL
  cast(null as varchar(16777216))    as HOUSEKEY,                   -- NOT NULL
  cast(null as varchar(16777216))    as CAMPAIGNCODE,               -- NOT NULL
  cast(null as varchar(16777216))    as CAMPAIGNNAME,
  cast(null as varchar(16777216))    as CAMPAIGNDESCRIPTION,
  cast(null as varchar(16777216))    as HISTORICALSELLINFIRSTMONTH,
  cast(null as varchar(16777216))    as HISTORICALSELLINLASTMONTH,
  cast(null as varchar(16777216))    as CAMPAIGNDATE,
  cast(null as timestamp_ltz)        as SYS_SOURCE_DATE,
  cast(current_timestamp() as timestamp_ltz) as SYS_DATE_CREATE
where false
