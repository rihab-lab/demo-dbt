{% macro create_streams_from_raw() %}
  {% set raw_tables = ['PRC_BENCHMARK_RAW','PRC_CAMPAIGN_RAW'] %}
  {% set database = var('SF_DATABASE') %}
  {% set schema   = 'RAW_LAYER' %}
  {% set results  = [] %}

  {# 1) Récupérer la liste des streams existants #}
  {% set show_streams_sql = "SHOW STREAMS IN SCHEMA " ~ database ~ "." ~ schema %}
  {% set show_result      = run_query(show_streams_sql) %}
  {# Ici, on extrait la première colonne (STREAM_NAME) #}
  {% set existing_streams = show_result.columns[1].values() %}

  {# 2) Logguer pour voir ce que contient “existing_streams” #}
  {% do log("→ Liste des streams existants: " ~ existing_streams | join(", "), info=True) %}

  {% for table in raw_tables %}
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
