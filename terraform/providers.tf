provider "snowflake" {
  alias                    = "security_admin"
  role                     = "SECURITYADMIN"
  preview_features_enabled = ["snowflake_table_resource"]

}

provider "snowflake" {
  alias                    = "sys_admin"
  role                     = "SYSADMIN"
  preview_features_enabled = ["snowflake_table_resource"]
}
provider "snowflake" {
  alias = "account_admin"
  role  = "ACCOUNTADMIN"
  preview_features_enabled = ["snowflake_table_resource", "snowflake_storage_integration_resource",
  "snowflake_file_format_resource", "snowflake_stage_resource", "snowflake_pipe_resource"] # mandatory to use preview resource "table creation"
}



