{{ config(
    materialized         = "incremental",
    schema               = "SILVER_LAYER",
    unique_key           = "PRICINGCAMPAIGNPRCINTKEY",
    incremental_strategy = "merge",
    on_schema_change     = "append_new_columns"
) }}

{%- set bronze_schema = 'BRONZE_LAYER' -%}
{%- set bronze_table  = 'PRC_CAMPAIGN_BRZ' -%}
{%- set bronze_stream = 'PRC_CAMPAIGN_BRZ_STREAM' -%}

{#-------------------------------
  1) Premier chargement (table n’existe pas ou full-refresh)
     → on lit la table Bronze (tout l’historique)
  2) En mode incrémental (table existe déjà)
     → on lit seulement le Stream (les deltas)
-------------------------------#}
{%- if not is_incremental() -%}
  with source as (
    select *
    from {{ source(bronze_schema, bronze_table) }}
  )
{%- else -%}
  with source as (
    select *
    from {{ source(bronze_schema, bronze_stream) }}
  )
{%- endif %}

, deduplicated as (
  select *,
    row_number() over (
      partition by HOUSEKEY, CAMPAIGNCODE
      order by LOAD_TIME desc
    ) as row_num
  from source
)

select
  hash(HOUSEKEY || '_' || CAMPAIGNCODE)                            as PRICINGCAMPAIGNPRCINTKEY,
  coalesce(CAMPAIGNCODE, 'N/A') || '_' || coalesce(CAMPAIGNNAME, 'N/A') as PRICINGCAMPAIGNPRCKEY,
  HOUSEKEY,
  CAMPAIGNCODE,
  CAMPAIGNNAME,
  CAMPAIGNDESCRIPTION,
  HISTORICALSELLINFIRSTMONTH,
  HISTORICALSELLINLASTMONTH,
  CAMPAIGNDATE,
  cast(
    try_to_timestamp_tz(SYS_SOURCE_DATE, 'YYYY-MM-DD HH24:MI:SS.FF3 TZD')
    as timestamp_ltz
  )                                                                as SYS_SOURCE_DATE,
  current_timestamp()                                              as SYS_DATE_CREATE
from deduplicated
where row_num = 1

{% if execute and this is not none %}
  {% do add_primary_key_if_not_exists(this, 'PRICINGCAMPAIGNPRCINTKEY') %}
{% endif %}
