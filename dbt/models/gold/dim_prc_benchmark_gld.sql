-- dbt/models/gold/dim_prc_benchmark_gld.sql

-- On ajoute la PK en post-hook (métadonnée Snowflake)
{{ config(
    materialized = "table",
    schema       = "GOLD_LAYER",
    post_hook    = [
      "alter table {{ this }} add primary key (PrcPcsBenchmarkIntKey)"
    ]
) }}

select
  cast(null as number)               as PrcPcsBenchmarkIntKey,       
  cast(null as number)               as PrcPcsGenericProductIntKey,  -- NOT NULL
  cast(null as varchar)              as APUKCode,                    -- NOT NULL
  cast(null as varchar)              as SKUGroup,
  cast(null as varchar)              as Anabench2,
  cast(null as varchar)              as Anabench1,
  cast(null as varchar)              as HouseCode,
  cast(null as timestamp_ltz)        as SYS_SOURCE_DATE,
  cast(current_timestamp() as timestamp_ltz) as SYS_DATE_CREATE,
  cast(current_timestamp() as timestamp_ltz) as SYS_DATE_UPDATE  -- NOT NULL
where false
