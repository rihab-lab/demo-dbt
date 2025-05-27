Run dbt run-operation create_streams_from_bronze
13:28:03  Running with dbt=1.9.4
13:28:03  Registered adapter: snowflake=1.9.4
13:28:04  Found 8 models, 2 sources, 480 macros
13:28:05  Encountered an error while running operation: Database Error
  002003 (42S02): SQL compilation error:
  Object '***.INFORMATION_SCHEMA.STREAMS' does not exist or not authorized.
Error: Process completed with exit code 1.