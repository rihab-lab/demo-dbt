{{ config(
    materialized = "table",
    schema       = "BRONZE_LAYER",
    post_hook    = [
      -- on ajoute la PK en post-hook (métadonnée Snowflake)
      "alter table {{ this }} add primary key (HOUSEKEY, CAMPAIGNCODE)"
    ]
) }}

select
  cast(null as varchar(16777216))    as HOUSEKEY,                     -- NOT NULL
  cast(null as varchar(16777216))    as CAMPAIGNCODE,                 -- NOT NULL
  cast(null as varchar(16777216))    as CAMPAIGNNAME,
  cast(null as varchar(16777216))    as CAMPAIGNDESCRIPTION,
  cast(null as varchar(16777216))    as HISTORICALSELLINFIRSTMONTH,
  cast(null as varchar(16777216))    as HISTORICALSELLINLASTMONTH,
  cast(null as varchar(16777216))    as CAMPAIGNDATE,
  cast(null as varchar(16777216))    as SYS_SOURCE_DATE,
  cast(null as timestamp_ltz)        as CREATE_DATE,                  -- metadata COPY INTO
  cast(null as varchar(16777216))    as file_name,                    -- metadata COPY INTO
  cast(current_timestamp() as timestamp_ltz) as LOAD_TIME
where false
