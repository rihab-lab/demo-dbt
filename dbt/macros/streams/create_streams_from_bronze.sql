{% macro create_streams_from_bronze() %}
  {% set bronze_tables = ['PRC_BENCHMARK_BRZ','PRC_CAMPAIGN_BRZ'] %}
  {% set database = var('SF_DATABASE') %}
  {% set schema   = 'BRONZE_LAYER' %}
  {% set results  = [] %}

  {# 1. Récupère une seule fois la liste des streams existants #}
  {% set show_q = "SHOW STREAMS IN SCHEMA " ~ database ~ "." ~ schema %}
  {% set show_result = run_query(show_q) %}
  {% set existing_streams = show_result.columns[0].values() %}

  {% for table in bronze_tables %}
    {% set stream_name = table ~ '_STREAM' %}
    {% if stream_name not in existing_streams %}
      {% set create_sql %}
        CREATE OR REPLACE STREAM {{ database }}.{{ schema }}.{{ stream_name }}
        ON TABLE {{ database }}.{{ schema }}.{{ table }}
        APPEND_ONLY = FALSE;
      {% endset %}

      {% do log("Création du stream : " ~ stream_name, info=True) %}
      {% do run_query(create_sql) %}
      {% do results.append("Stream créé : " ~ stream_name) %}
    {% else %}
      {% do log("Stream déjà existant, skip : " ~ stream_name, info=True) %}
    {% endif %}
  {% endfor %}

  {% do return(results) %}
{% endmacro %}
