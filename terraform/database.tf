resource "snowflake_database" "db" {
  provider = snowflake.sys_admin
  name     = "TEST_POC_VISEO_DB"
  comment  = "Main dev database"
}

resource "snowflake_grant_privileges_to_account_role" "database_grant" {
  provider          = snowflake.security_admin
  privileges        = ["USAGE", "MONITOR", "MODIFY", "CREATE SCHEMA"]
  account_role_name = snowflake_account_role.role.name
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.db.name
  }
}


resource "snowflake_schema" "bronze_layer" {
  provider            = snowflake.sys_admin
  database            = snowflake_database.db.name
  name                = "BRONZE_LAYER"
  with_managed_access = false
}

resource "snowflake_grant_privileges_to_account_role" "schema_grant_bronze" {
  provider          = snowflake.security_admin
  privileges        = ["USAGE", "CREATE TABLE", "CREATE VIEW", "CREATE PROCEDURE", "CREATE NOTEBOOK", "CREATE STAGE", "CREATE FILE FORMAT", "CREATE TASK", "CREATE STREAM", "CREATE PIPE", "CREATE SEQUENCE"]
  account_role_name = snowflake_account_role.role.name
  on_schema {
    schema_name = "\"${snowflake_database.db.name}\".\"${snowflake_schema.bronze_layer.name}\""
  }
}


resource "snowflake_schema" "raw_layer" {
  provider            = snowflake.sys_admin
  database            = snowflake_database.db.name
  name                = "RAW_LAYER"
  with_managed_access = false
}

resource "snowflake_grant_privileges_to_account_role" "schema_grant_raw" {
  provider          = snowflake.security_admin
  privileges        = ["USAGE", "CREATE EXTERNAL TABLE", "CREATE TABLE", "CREATE VIEW", "CREATE PROCEDURE", "CREATE NOTEBOOK", "CREATE STAGE", "CREATE FILE FORMAT", "CREATE TASK", "CREATE STREAM", "CREATE PIPE"]
  account_role_name = snowflake_account_role.role.name
  on_schema {
    schema_name = "\"${snowflake_database.db.name}\".\"${snowflake_schema.raw_layer.name}\""
  }
}


resource "snowflake_schema" "silver_layer" {
  provider            = snowflake.sys_admin
  database            = snowflake_database.db.name
  name                = "SILVER_LAYER"
  with_managed_access = false
}

resource "snowflake_grant_privileges_to_account_role" "schema_grant_silver" {
  provider          = snowflake.security_admin
  privileges        = ["USAGE", "CREATE TABLE", "CREATE VIEW", "CREATE PROCEDURE", "CREATE NOTEBOOK", "CREATE STAGE", "CREATE FILE FORMAT", "CREATE TASK", "CREATE STREAM", "CREATE PIPE"]
  account_role_name = snowflake_account_role.role.name
  on_schema {
    schema_name = "\"${snowflake_database.db.name}\".\"${snowflake_schema.silver_layer.name}\""
  }
}

resource "snowflake_schema" "gold_layer" {
  provider            = snowflake.sys_admin
  database            = snowflake_database.db.name
  name                = "GOLD_LAYER"
  with_managed_access = false
}

resource "snowflake_grant_privileges_to_account_role" "schema_grant_gold" {
  provider          = snowflake.security_admin
  privileges        = ["USAGE", "CREATE TABLE", "CREATE VIEW", "CREATE PROCEDURE", "CREATE NOTEBOOK", "CREATE STAGE", "CREATE FILE FORMAT", "CREATE TASK", "CREATE STREAM", "CREATE PIPE"]
  account_role_name = snowflake_account_role.role.name
  on_schema {
    schema_name = "\"${snowflake_database.db.name}\".\"${snowflake_schema.gold_layer.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "future_tables_grant" {
  provider          = snowflake.security_admin
  privileges        = ["SELECT", "INSERT"]
  account_role_name = snowflake_account_role.role.name
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_database        = snowflake_database.db.name
    }
  }
}


resource "snowflake_schema" "monitoring_layer" {
  provider            = snowflake.sys_admin
  database            = snowflake_database.db.name
  name                = "MONITORING_LAYER"
  with_managed_access = false
}

resource "snowflake_grant_privileges_to_account_role" "schema_grant_monitoring" {
  provider          = snowflake.security_admin
  privileges        = ["USAGE", "CREATE TABLE", "CREATE VIEW", "CREATE PROCEDURE", "CREATE NOTEBOOK", "CREATE STAGE", "CREATE FILE FORMAT", "CREATE TASK", "CREATE STREAM", "CREATE PIPE", "CREATE SEQUENCE"]
  account_role_name = snowflake_account_role.role.name
  on_schema {
    schema_name = "\"${snowflake_database.db.name}\".\"${snowflake_schema.monitoring_layer.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "executetask" {
  provider          = snowflake.account_admin
  account_role_name = snowflake_account_role.role.name
  privileges        = ["EXECUTE TASK", "MONITOR EXECUTION", "EXECUTE MANAGED TASK"]
  on_account        = true

}