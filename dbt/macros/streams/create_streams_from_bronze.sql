{% macro create_streams_from_bronze() %}
  {% set bronze_tables = [
    'PRC_BENCHMARK_BRZ',
    'PRC_CAMPAIGN_BRZ'
  ] %}

  {% set schema = 'BRONZE_LAYER' %}
  {% set database = var('SF_DATABASE') %}
  {% set results = [] %}

  {% for table in bronze_tables %}
    {% set stream_name = table ~ '_STREAM' %}

    {# Vérifie si le stream existe déjà dans information_schema.streams #}
    {% set check_stream_query %}
      select count(*) from {{ database }}.information_schema.streams
      where stream_name = upper('{{ stream_name }}')
        and stream_schema = upper('{{ schema }}')
        and stream_catalog = upper('{{ database }}')
    {% endset %}

    {% set check_result = run_query(check_stream_query) %}
    {% set exists = check_result.columns[0].values()[0] %}

    {% if exists == 0 %}
      {% set sql %}
        create or replace stream {{ database }}.{{ schema }}.{{ stream_name }}
        on table {{ database }}.{{ schema }}.{{ table }}
        append_only = false;
      {% endset %}

      {% do log("✅ Création du stream : " ~ stream_name, info=True) %}
      {% do run_query(sql) %}
      {% do results.append("✅ Stream créé : " ~ stream_name) %}
    {% else %}
      {% do log("ℹ️ Stream déjà existant : " ~ stream_name ~ ", ignoré", info=True) %}
    {% endif %}
  {% endfor %}

  {% do return(results) %}
{% endmacro %}
