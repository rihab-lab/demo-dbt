resource "snowflake_account_role" "role" {
  provider = snowflake.security_admin
  name     = "TEST_POC_VISEO_ROLE"
  comment  = "Main role for Snowflake POC account usage"
}