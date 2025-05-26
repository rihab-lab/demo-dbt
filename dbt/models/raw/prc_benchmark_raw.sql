{{ 
  config(
    materialized = "table",
    database     = "TEST_POC_VISEO_DB",
    schema       = "RAW_LAYER",
    post_hook    = [
      "{{ copy_into_raw(
           table_name     = this.identifier,
           prefix_pattern = 'PRC_BENCHMARK',
           columns        = [
             'APUKCODE',
             'ANABENCH2CODE',
             'ANABENCH2',
             'SKUGROUP',
             'FILE_NAME',
             'SYS_SOURCE_DATE'
           ],
           select_exprs   = [
             't.$1          AS APUKCODE',
             't.$2          AS ANABENCH2CODE',
             't.$3          AS ANABENCH2',
             't.$4          AS SKUGROUP',
             'METADATA$FILENAME   AS FILE_NAME',
             'METADATA$CREATED_ON AS SYS_SOURCE_DATE'
           ]
         ) }}"
    ]
  ) 
}}

select
  cast(null as varchar(16777216)) as APUKCode,
  cast(null as varchar(16777216)) as Anabench2Code,
  cast(null as varchar(16777216)) as Anabench2,
  cast(null as varchar(16777216)) as SKUGroup,
  cast(null as varchar(16777216)) as FILE_NAME,
  cast(null as timestamp_ltz)     as SYS_SOURCE_DATE
where false
