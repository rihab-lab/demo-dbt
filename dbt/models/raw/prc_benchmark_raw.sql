{{ config(
    materialized = 'table',
    database     = 'TEST_POC_VISEO_DB',   -- explicitement, si ta db par défaut n’est pas celle-ci
    schema       = 'RAW_LAYER'
) }}

-- CTAS vide : on crée seulement la structure, sans ligne
select
  cast(null as varchar(16777216))     as APUKCode,
  cast(null as varchar(16777216))     as Anabench2Code,
  cast(null as varchar(16777216))     as Anabench2,
  cast(null as varchar(16777216))     as SKUGroup,
  cast(null as varchar(16777216))     as SYS_SOURCE_DATE,
  cast(null as varchar(16777216))     as FILE_NAME,
  cast(current_timestamp() as timestamp_ntz(9)) as LOAD_TIME
where false
