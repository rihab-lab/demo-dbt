{% macro generate_schema_name(custom_schema=None, node=None, root_project=None) -%}
  {%- if custom_schema -%}
    {{ return(custom_schema) }}
  {%- endif -%}
  {{ return(target.schema) }}
{%- endmacro %}
