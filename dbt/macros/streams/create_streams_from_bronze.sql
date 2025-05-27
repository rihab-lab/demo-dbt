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

    {# Liste tous les streams dans le schéma concerné #}
    {% set show_streams_query %}
      show streams in schema {{ database }}.{{ schema }}
    {% endset %}

    {% set show_result = run_query(show_streams_query) %}
    {% set stream_names = show_result.columns[0].values() %}
    {% set exists = stream_name in stream_names %}

    {% if not exists %}
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
