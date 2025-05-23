-- dbt/models/bronze/prc_benchmark_brz.sql

-- On ajoute la PK en post-hook (métadonnée Snowflake)
{{ config(
    materialized = "table",
    schema       = "BRONZE_LAYER",
    post_hook    = [
      "alter table {{ this }} add primary key (APUKCode, Anabench2Code)"
    ]
) }}

select
  cast(null as varchar(16777216)) as APUKCode,
  cast(null as varchar(16777216)) as Anabench2Code,
  cast(null as varchar(16777216)) as Anabench2,
  cast(null as varchar(16777216)) as SKUGroup,
  cast(null as varchar(16777216)) as SYS_SOURCE_DATE,
  cast(null as timestamp_ltz)     as CREATE_DATE,
  cast(null as varchar(16777216)) as file_name,
  cast(current_timestamp() as timestamp_ltz) as LOAD_TIME
where false
