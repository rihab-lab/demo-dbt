-- dbt/models/raw/prc_benchmark_load.sql

{{ config(
    materialized = "script",
    database     = "TEST_POC_VISEO_DB",
    schema       = "RAW_LAYER"
) }}

{{ copy_into_raw(
     table_name     = 'PRC_BENCHMARK_RAW',
     prefix_pattern = 'PRC_BENCHMARK'
) }}
