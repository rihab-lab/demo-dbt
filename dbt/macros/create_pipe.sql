-- macros/create_pipes.sql

{% macro create_raw_pipes() %}
{% set db    = target.database %}
{% set sch   = 'RAW_LAYER' %}
{% set stage = db ~ '.' ~ sch ~ '.EXTERNAL_AZURE_STAGE' %}
{% set fmt   = sch ~ '.FF_CSV' %}

-- Pipe pour PRC_BENCHMARK_RAW
CREATE OR REPLACE PIPE {{ db }}.{{ sch }}.PRC_BENCHMARK_PIPE
  AUTO_INGEST = TRUE
  AS
  COPY INTO {{ db }}.{{ sch }}.PRC_BENCHMARK_RAW
    (APUKCode, Anabench2Code, Anabench2, SKUGroup, FILE_NAME, SYS_SOURCE_DATE)
  FROM (
    SELECT
      $1::VARCHAR           AS APUKCode,
      $2::VARCHAR           AS Anabench2Code,
      $3::VARCHAR           AS Anabench2,
      $4::VARCHAR           AS SKUGroup,
      metadata$filename     AS FILE_NAME,
      metadata$created_on   AS SYS_SOURCE_DATE
    FROM @{{ stage }}
    WHERE metadata$filename LIKE 'PRC_BENCHMARK_%'
  )
  FILE_FORMAT = (FORMAT_NAME = '{{ fmt }}')
  ON_ERROR    = 'CONTINUE'
;

-- Pipe pour PRC_CAMPAIGN_RAW
CREATE OR REPLACE PIPE {{ db }}.{{ sch }}.PRC_CAMPAIGN_PIPE
  AUTO_INGEST = TRUE
  AS
  COPY INTO {{ db }}.{{ sch }}.PRC_CAMPAIGN_RAW
    (HouseKey, CampaignCode, CampaignName, CampaignDescription,
     HistoricalSellInFirstMonth, HistoricalSellInLastMonth,
     CampaignDate, FILE_NAME, SYS_SOURCE_DATE)
  FROM (
    SELECT
      $1::VARCHAR           AS HouseKey,
      $2::VARCHAR           AS CampaignCode,
      $3::VARCHAR           AS CampaignName,
      $4::VARCHAR           AS CampaignDescription,
      $5::VARCHAR           AS HistoricalSellInFirstMonth,
      $6::VARCHAR           AS HistoricalSellInLastMonth,
      $7::VARCHAR           AS CampaignDate,
      metadata$filename     AS FILE_NAME,
      metadata$created_on   AS SYS_SOURCE_DATE
    FROM @{{ stage }}
    WHERE metadata$filename LIKE 'PRC_CAMPAIGN_%'
  )
  FILE_FORMAT = (FORMAT_NAME = '{{ fmt }}')
  ON_ERROR    = 'CONTINUE'
;
{% endmacro %}
