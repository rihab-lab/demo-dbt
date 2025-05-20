/*resource "snowflake_pipe" "pipe_raw_prc_benchmark" {
  name        = "PIPE_RAW_PRC_BENCHMARK"
  provider    = snowflake.account_admin
  database    = snowflake_database.db.name
  schema      = snowflake_schema.raw_layer.name
  auto_ingest = false

  copy_statement = <<-SQL
  COPY INTO TEST_POC_VISEO_DB.RAW_LAYER.PRC_BENCHMARK_RAW (
  APUKCODE ,
  ANABENCH2CODE,
  ANABENCH2 ,
  SKUGROUP ,
  SYS_SOURCE_DATE ,
  FILE_NAME ,
  LOAD_TIME 
)
FROM (
  SELECT
    $1,                         -- APUKCode
    $2,                         -- Anabench2Code
    $3,                         -- Anabench2
    $4,                         -- SKUGroup
    $5,
    METADATA$FILENAME,         -- FILE_NAME
    CURRENT_TIMESTAMP()        -- LOAD_TIME
  FROM @TEST_POC_VISEO_DB.RAW_LAYER.EXTERNAL_AZURE_STAGE
)
FILE_FORMAT = (
  TYPE = 'CSV'
  FIELD_DELIMITER = ';'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
)
PATTERN = '.*PRC_BENCHMARK_[0-9]{8}\\.csv';
  SQL
}


resource "snowflake_pipe" "pipe_raw_prc_campaign" {
  name        = "PIPE_RAW_PRC_CAMPAIGN"
  provider    = snowflake.account_admin
  database    = snowflake_database.db.name
  schema      = snowflake_schema.raw_layer.name
  auto_ingest = false

  copy_statement = <<-SQL
  COPY INTO TEST_POC_VISEO_DB.RAW_LAYER.PRC_CAMPAIGN_RAW(
    HOUSEKEY ,
	CAMPAIGNCODE ,
	CAMPAIGNNAME ,
	CAMPAIGNDESCRIPTION ,
	HISTORICALSELLINFIRSTMONTH ,
	HISTORICALSELLINLASTMONTH ,
	CAMPAIGNDATE ,
	SYS_SOURCE_DATE ,
	FILE_NAME ,
	LOAD_TIME 
)
FROM (
  SELECT
    $1, 
    $2,                         
    $3,                      
    $4,                         
    $5,
    $6,
    $7,
    $8,
    METADATA$FILENAME,         -- FILE_NAME
    CURRENT_TIMESTAMP()        -- LOAD_TIME
  FROM @TEST_POC_VISEO_DB.RAW_LAYER.EXTERNAL_AZURE_STAGE
)
FILE_FORMAT = (
  TYPE = 'CSV'
  FIELD_DELIMITER = ';'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
)
PATTERN = '.*PRC_CAMPAIGN_[0-9]{8}\\.csv';
  SQL
}*/