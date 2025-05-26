{% macro copy_into_raw(table_name, prefix_pattern) %}
COPY INTO {{ this.database }}.{{ this.schema }}.{{ table_name | upper }}
FROM (
  SELECT
    t.*,
    METADATA$FILENAME   AS FILE_NAME,
    METADATA$CREATED_ON AS SYS_SOURCE_DATE
  FROM @{{ this.database }}.{{ this.schema }}.EXTERNAL_AZURE_STAGE t
  WHERE metadata$filename LIKE '{{ prefix_pattern }}_%'
)
FILE_FORMAT = (format_name = '{{ this.schema }}.FF_CSV')
ON_ERROR = 'continue';
{% endmacro %}
