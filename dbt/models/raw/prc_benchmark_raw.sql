{{ 
  config(
    materialized = "table",
    database     = "TEST_POC_VISEO_DB",
    schema       = "RAW_LAYER",
    post_hook    = [
      "{{ copy_into_raw(
           table_name     = this.identifier,
           prefix_pattern = 'PRC_BENCHMARK'
         ) }}"
    ]
  ) 
}}
select
  cast(null as varchar(16777216))     as APUKCode,
  cast(null as varchar(16777216))     as Anabench2Code,
  cast(null as varchar(16777216))     as Anabench2,
  cast(null as varchar(16777216))     as SKUGroup,
  cast(null as varchar(16777216))     as SYS_SOURCE_DATE,
  cast(null as varchar(16777216))     as FILE_NAME,
  cast(current_timestamp() as timestamp_ntz(9)) as LOAD_TIME
where false
