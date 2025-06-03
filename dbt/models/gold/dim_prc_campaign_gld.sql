{{ 
  config(
    materialized         = "incremental",
    schema               = "GOLD_LAYER",
    unique_key           = "PrcPcsCampaignIntKey",
    incremental_strategy = "merge",
    on_schema_change     = "append_new_columns"
  ) 
}}

with source as (
  select
    PricingCampaignPrcIntKey    as PrcPcsCampaignIntKey,
    HouseKey,
    CampaignCode,
    current_timestamp()         as SYS_DATE_CREATE,
    current_timestamp()         as SYS_DATE_UPDATE,
    row_number() over (
      partition by PricingCampaignPrcIntKey
      order by SYS_DATE_UPDATE desc
    ) as row_num
   from {{ source('SILVER_LAYER', 'dim_prc_campaign_slv_stream') }}
  where
    PricingCampaignPrcIntKey is not null
    and HouseKey           is not null
    and CampaignCode       is not null
),

deduplicated as (
  select *
  from source
  where row_num = 1
)

select
  PrcPcsCampaignIntKey,
  HouseKey,
  CampaignCode,
  SYS_DATE_CREATE,
  SYS_DATE_UPDATE
from deduplicated


{% if execute and this is not none %}
  {% do add_primary_key_if_not_exists(this, 'PrcPcsCampaignIntKey') %}
{% endif %}