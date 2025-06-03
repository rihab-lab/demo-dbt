{% macro create_streams_from_bronze() %}
  {# Liste des tables Bronze pour lesquelles on veut un stream #}
  {% set bronze_tables = [
    'PRC_BENCHMARK_BRZ',
    'PRC_CAMPAIGN_BRZ'
  ] %}

  {# Variables de contexte Snowflake #}
  {% set database = var('SF_DATABASE') %}
  {% set schema   = 'BRONZE_LAYER' %}
  {% set results  = [] %}

  {# ————————————————————————————————————————————————————————————————————————————————
     1) On récupère en une seule fois la liste des streams déjà existants
     via la commande SHOW STREAMS IN SCHEMA <database>.<schema>
     Cela retourne une table avec, notamment, la colonne STREAM_NAME.
  ———————————————————————————————————————————————————————————————————————————————— #}
  {% set show_streams_sql = "SHOW STREAMS IN SCHEMA " ~ database ~ "." ~ schema %}
  {% set show_result      = run_query(show_streams_sql) %}
  {% set existing_streams = show_result.columns[0].values() %}

  {# Boucle sur chaque table Bronze #}
  {% for table in bronze_tables %}
    {% set stream_name = table ~ '_STREAM' %}

    {% if stream_name not in existing_streams %}
      {# Si le stream n’existe pas, on le crée #}
      {% set create_stream_sql %}
        CREATE OR REPLACE STREAM {{ database }}.{{ schema }}.{{ stream_name }}
        ON TABLE {{ database }}.{{ schema }}.{{ table }}
        APPEND_ONLY = FALSE;
      {% endset %}

      {% do log("Création du stream : " ~ stream_name, info=True) %}
      {% do run_query(create_stream_sql) %}
      {% do results.append("Stream créé : " ~ stream_name) %}
    {% else %}
      {# Si le stream existe déjà, on ne fait rien #}
      {% do log("Stream déjà existant, skip : " ~ stream_name, info=True) %}
    {% endif %}
  {% endfor %}

  {# On retourne une liste de messages pour voir ce qui a été fait #}
  {% do return(results) %}
{% endmacro %}
