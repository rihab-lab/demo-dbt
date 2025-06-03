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

    {# Show streams from the schema #}
    {% set show_query %}
      show streams in schema {{ database }}.{{ schema }}
    {% endset %}

    {% set show_result = run_query(show_query) %}
    {% set stream_exists = false %}

    {% if show_result %}
      {% for row in show_result.rows %}
        {% if row['name'] | upper == stream_name %}
          {% set stream_exists = true %}
        {% endif %}
      {% endfor %}
    {% endif %}

    {% if not stream_exists %}
      {% set create_sql %}
        create or replace stream {{ database }}.{{ schema }}.{{ stream_name }}
        on table {{ database }}.{{ schema }}.{{ table }}
        append_only = false;
      {% endset %}

      {% do log("✅ Création du stream : " ~ stream_name, info=True) %}
      {% do run_query(create_sql) %}
      {% do results.append("Stream créé : " ~ stream_name) %}
    {% else %}
      {% do log("ℹ️ Stream déjà existant : " ~ stream_name ~ ", ignoré", info=True) %}
    {% endif %}
  {% endfor %}

  {% do return(results) %}
{% endmacro %}
