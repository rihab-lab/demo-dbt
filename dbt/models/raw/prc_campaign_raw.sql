{{ config(
    materialized='table',
    schema='RAW_LAYER'
) }}

SELECT
    CAST(NULL AS STRING)         AS HouseKey,
    CAST(NULL AS STRING)         AS CampaignCode,
    CAST(NULL AS STRING)         AS CampaignName,
    CAST(NULL AS STRING)         AS CampaignDescription,
    CAST(NULL AS STRING)         AS HistoricalSellInFirstMonth,
    CAST(NULL AS STRING)         AS HistoricalSellInLastMonth,
    CAST(NULL AS STRING)         AS CampaignDate,
    CAST(NULL AS STRING)         AS SYS_SOURCE_DATE,
    CAST(NULL AS STRING)         AS FILE_NAME,
    CAST(NULL AS TIMESTAMP_NTZ)  AS LOAD_TIME
WHERE FALSE
