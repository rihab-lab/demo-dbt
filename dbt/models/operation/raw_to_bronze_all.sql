{{  
  config(
    materialized='operation',
    tags=['operation'],
    post_hook=
      "CREATE OR REPLACE PROCEDURE raw_to_bronze_all()\n" ||
      "  RETURNS VARCHAR\n" ||
      "  LANGUAGE SQL\n" ||
      "  EXECUTE AS OWNER\n" ||
      "AS\n" ||
      "$$\n" ||
      render_all_merges() ||
      "\n$$;"
  )
}}

{% macro render_all_merges() %}
  {% for pipe in var('pipe_configs') %}
    {% do log("Merging " ~ pipe.table, info=true) %}
    {{  -- on “appelle” ta macro pour générer le SQL de merge incrémental
       generate_bronze_merge_model(
         pipe.table,
         pipe.unique_key
       )
    }}
    ;
  {% endfor %}
  RETURN 'DONE';
{% endmacro %}
