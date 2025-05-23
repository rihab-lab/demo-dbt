-- dbt/models/silver/dim_prc_benchmark_slv.sql

-- On ajoute la clé primaire en post-hook (métadonnée Snowflake)
{{ config(
    materialized = "table",
    schema       = "SILVER_LAYER",
    post_hook    = [
      "alter table {{ this }} add primary key (PRICINGBENCHMARKPRCINTKEY)"
    ]
) }}

select
  cast(null as number)               as PRICINGBENCHMARKPRCINTKEY,  -- NOT NULL
  cast(null as varchar)              as PRICINGBENCHMARKPRCKEY,     -- NOT NULL
  cast(null as varchar)              as APUKCODE,                   -- NOT NULL
  cast(null as varchar)              as ANABENCH2,                  -- NOT NULL
  cast(null as varchar)              as SKUGROUP,
  cast(null as varchar)              as SYS_SOURCE_DATE,
  cast(current_timestamp() as timestamp_ltz(9)) as SYS_DATE_CREATE  -- NOT NULL
where false
