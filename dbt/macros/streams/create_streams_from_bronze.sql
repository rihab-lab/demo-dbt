{% macro create_streams_from_bronze() %}
  {% set bronze_tables = ['PRC_BENCHMARK_BRZ', 'PRC_CAMPAIGN_BRZ'] %}
  {% set schema = 'BRONZE_LAYER' %}
  {% set database = var('SF_DATABASE') %}
  {% set results = [] %}

  {% for table in bronze_tables %}
    {% set stream_name = table ~ '_STREAM' %}
    {% set stream_exists = false %}

    {% set show_query %}
      show streams in schema {{ database }}.{{ schema }}
    {% endset %}
    {% set show_result = run_query(show_query) %}

    {% for row in show_result.rows %}
      {% if row[1] is not none and row[1] | upper == stream_name %}
        {% set stream_exists = true %}
      {% endif %}
    {% endfor %}

    {% if not stream_exists %}
      {% set sql %}
        create or replace stream {{ database }}.{{ schema }}.{{ stream_name }}
        on table {{ database }}.{{ schema }}.{{ table }}
        append_only = false;
      {% endset %}

      {% do log("✅ Création du stream : " ~ stream_name, info=True) %}
      {% do run_query(sql) %}
      {% do results.append("Stream créé : " ~ stream_name) %}
    {% else %}
      {% do log("ℹ️ Stream déjà existant : " ~ stream_name ~ ", ignoré", info=True) %}
      {% do results.append("Stream ignoré : " ~ stream_name) %}
    {% endif %}
  {% endfor %}

  {% do return(results) %}
{% endmacro %}
