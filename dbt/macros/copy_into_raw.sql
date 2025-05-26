{% macro copy_into_raw(table_name, prefix_pattern) %}
copy into {{ this.database }}.{{ this.schema }}.{{ table_name | upper }}
from (
  select
    t.*,
    metadata$filename   as FILE_NAME,
    metadata$created_on as SYS_SOURCE_DATE
  from @{{ this.database }}.{{ this.schema }}.EXTERNAL_AZURE_STAGE t
  where metadata$filename like '{{ prefix_pattern }}_%'
)
file_format = (format_name => '{{ this.schema }}.FF_CSV')
on_error = 'continue';
{% endmacro %}
