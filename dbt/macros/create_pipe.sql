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
  FROM {{ stage }}
    PATTERN = 'PRC_BENCHMARK_.*\\.csv'
    FILE_FORMAT = (FORMAT_NAME = '{{ fmt }}')
    ON_ERROR    = 'CONTINUE'
;

-- Pipe pour PRC_CAMPAIGN_RAW
CREATE OR REPLACE PIPE {{ db }}.{{ sch }}.PRC_CAMPAIGN_PIPE
  AUTO_INGEST = TRUE
  AS
  COPY INTO {{ db }}.{{ sch }}.PRC_CAMPAIGN_RAW
  FROM {{ stage }}
    PATTERN = 'PRC_CAMPAIGN_.*\\.csv'
    FILE_FORMAT = (FORMAT_NAME = '{{ fmt }}')
    ON_ERROR    = 'CONTINUE'
;
{% endmacro %}
