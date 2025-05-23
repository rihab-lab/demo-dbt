{{ config(
    materialized = "table",
    schema       = "BRONZE_LAYER",
    post_hook    = [
      -- Snowflake n’applique pas physiquement la PK, mais on la stocke en métadonnée
      "alter table {{ this }} add primary key (APUKCode, Anabench2Code)"
    ]
) }}

-- CTAS vide : on ne crée que la structure, sans lignes
select
  cast(null as varchar) as APUKCode,            -- NOT NULL
  cast(null as varchar) as Anabench2Code,      -- NOT NULL
  cast(null as varchar) as Anabench2,
  cast(null as varchar) as SKUGroup,
  cast(null as varchar) as SYS_SOURCE_DATE,
  cast(null as timestamp_ltz) as CREATE_DATE,  -- Copy into metadata
  cast(null as varchar)   as file_name,        -- Copy into metadata
  cast(current_timestamp() as timestamp_ltz) as LOAD_TIME
where false
