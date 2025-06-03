{% macro create_streams_from_bronze() %}
  {% set bronze_tables = ['PRC_BENCHMARK_BRZ', 'PRC_CAMPAIGN_BRZ'] %}
  {% set schema = 'BRONZE_LAYER' %}
  {% set database = var('SF_DATABASE') %}
  {% set results = [] %}

  {% for table in bronze_tables %}
    {% set stream_name = table ~ '_STREAM' %}

    {% set check_query %}
      select count(*) as count
      from {{ database }}.information_schema.streams
      where stream_name = upper('{{ stream_name }}')
        and table_name = upper('{{ table }}')
        and schema_name = upper('{{ schema }}')
    {% endset %}

    {% set check_result = run_query(check_query) %}
    {% if execute %}
      {% set count = check_result.columns[0].values()[0] %}
    {% else %}
      {% set count = 0 %}
    {% endif %}

    {% if count == 0 %}
      {% set create_query %}
        create or replace stream {{ database }}.{{ schema }}.{{ stream_name }}
        on table {{ database }}.{{ schema }}.{{ table }}
        append_only = false;
      {% endset %}
      
      {% do run_query(create_query) %}
      {% do log("Stream créé : " ~ stream_name, info=True) %}
      {% do results.append("Créé : " ~ stream_name) %}
    {% else %}
      {% do log("Stream déjà existant : " ~ stream_name, info=True) %}
      {% do results.append("Déjà existant : " ~ stream_name) %}
    {% endif %}
  {% endfor %}

  {% do return(results) %}
{% endmacro %}
