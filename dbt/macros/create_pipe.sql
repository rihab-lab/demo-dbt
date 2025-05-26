{% macro create_raw_pipes() %}
{% set db = target.database %}
{% set sch = 'RAW_LAYER' %}
{% set stage = db ~ '.' ~ sch ~ '.EXTERNAL_AZURE_STAGE' %}
{% set fmt   = sch ~ '.FF_CSV' %}

-- Pipe pour PRC_BENCHMARK_RAW
create or replace pipe {{ db }}.{{ sch }}.PRC_BENCHMARK_PIPE
  auto_ingest = true
  as
  copy into {{ db }}.{{ sch }}.PRC_BENCHMARK_RAW
    (APUKCode, Anabench2Code, Anabench2, SKUGroup, FILE_NAME, SYS_SOURCE_DATE)
  from (
    select
      $1::varchar           as APUKCode,
      $2::varchar           as Anabench2Code,
      $3::varchar           as Anabench2,
      $4::varchar           as SKUGroup,
      metadata$filename     as FILE_NAME,
      metadata$created_on   as SYS_SOURCE_DATE
    from @{{ stage }}
    where metadata$filename like 'PRC_BENCHMARK_%'
  )
  file_format = (format_name => '{{ fmt }}')
  on_error = 'continue';

-- Pipe pour PRC_CAMPAIGN_RAW
create or replace pipe {{ db }}.{{ sch }}.PRC_CAMPAIGN_PIPE
  auto_ingest = true
  as
  copy into {{ db }}.{{ sch }}.PRC_CAMPAIGN_RAW
    (HouseKey, CampaignCode, CampaignName, CampaignDescription,
     HistoricalSellInFirstMonth, HistoricalSellInLastMonth,
     CampaignDate, FILE_NAME, SYS_SOURCE_DATE)
  from (
    select
      $1::varchar           as HouseKey,
      $2::varchar           as CampaignCode,
      $3::varchar           as CampaignName,
      $4::varchar           as CampaignDescription,
      $5::varchar           as HistoricalSellInFirstMonth,
      $6::varchar           as HistoricalSellInLastMonth,
      $7::varchar           as CampaignDate,
      metadata$filename     as FILE_NAME,
      metadata$created_on   as SYS_SOURCE_DATE
    from @{{ stage }}
    where metadata$filename like 'PRC_CAMPAIGN_%'
  )
  file_format = (format_name => '{{ fmt }}')
  on_error = 'continue';
{% endmacro %}
