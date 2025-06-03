/*{{ config(
    materialized = "incremental",
    schema = "SILVER_LAYER",
    unique_key = "PRICINGCAMPAIGNPRCINTKEY",
    incremental_strategy = "merge",
    on_schema_change = "append_new_columns"
) }}

with source as (
    select *
    from {{ source('BRONZE_LAYER', 'PRC_CAMPAIGN_BRZ_STREAM') }}
),

deduplicated as (
    select *,
           row_number() over (
               partition by HOUSEKEY, CAMPAIGNCODE
               order by LOAD_TIME desc
           ) as row_num
    from source
)

select
    hash(HOUSEKEY || '_' || CAMPAIGNCODE) as PRICINGCAMPAIGNPRCINTKEY,
    coalesce(CAMPAIGNCODE, 'N/A') || '_' || coalesce(CAMPAIGNNAME, 'N/A') as PRICINGCAMPAIGNPRCKEY,
    HOUSEKEY,
    CAMPAIGNCODE,
    CAMPAIGNNAME,
    CAMPAIGNDESCRIPTION,
    HISTORICALSELLINFIRSTMONTH,
    HISTORICALSELLINLASTMONTH,
    CAMPAIGNDATE,
    cast(try_to_timestamp_tz(SYS_SOURCE_DATE, 'YYYY-MM-DD HH24:MI:SS.FF3 TZD') as timestamp_ltz) as SYS_SOURCE_DATE,
    current_timestamp() as SYS_DATE_CREATE
from deduplicated
where row_num = 1

{% if execute and this is not none %}
  {{ add_primary_key_if_not_exists(this, 'PRICINGCAMPAIGNPRCINTKEY') }}
{% endif %}
*/