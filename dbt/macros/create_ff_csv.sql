-- dbt/macros/create_ff_csv.sql
{% macro create_ff_csv() %}
CREATE OR REPLACE FILE FORMAT RAW_LAYER.FF_CSV
  TYPE = 'CSV'
  FIELD_DELIMITER = ';'
  SKIP_HEADER = 1
  NULL_IF = ('', 'NULL');
{% endmacro %}
