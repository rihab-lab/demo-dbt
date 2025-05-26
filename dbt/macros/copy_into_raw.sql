{% macro copy_into_raw(table_name, prefix_pattern) %}
COPY INTO {{ this.database }}.{{ this.schema }}.{{ table_name | upper }}
(
  APUKCODE,
  ANABENCH2CODE,
  ANABENCH2,
  SKUGROUP,
  FILE_NAME,
  SYS_SOURCE_DATE
)
SELECT
  t.$1                   AS APUKCODE,
  t.$2                   AS ANABENCH2CODE,
  t.$3                   AS ANABENCH2,
  t.$4                   AS SKUGROUP,
  METADATA$FILENAME      AS FILE_NAME,
  METADATA$CREATED_ON    AS SYS_SOURCE_DATE
FROM @{{ this.database }}.{{ this.schema }}.EXTERNAL_AZURE_STAGE t
WHERE METADATA$FILENAME LIKE '{{ prefix_pattern }}_%'
FILE_FORMAT = (format_name = '{{ this.schema }}.FF_CSV')
ON_ERROR = 'CONTINUE';
{% endmacro %}
