-- dbt/macros/copy_into_raw.sql

{% macro copy_into_raw(table_name, prefix_pattern, columns, select_exprs) %}
COPY INTO {{ this.database }}.{{ this.schema }}.{{ table_name | upper }}
(
  {{ columns | join(",\n  ") }}
)
SELECT
  {{ select_exprs | join(",\n  ") }}
FROM @{{ this.database }}.{{ this.schema }}.EXTERNAL_AZURE_STAGE t
WHERE metadata$filename LIKE '{{ prefix_pattern }}_%'
FILE_FORMAT = (format_name = '{{ this.schema }}.FF_CSV')
ON_ERROR = 'CONTINUE';
{% endmacro %}
