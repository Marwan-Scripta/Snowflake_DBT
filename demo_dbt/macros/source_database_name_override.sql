---- This macro allows you to dynamically set the database name for a source based on the target environment.

{% macro my_dynamic_source(source_name, table_name) %}
  {% set s = source(source_name, table_name) %}
  {% set db = s.database %}
  {% set schema = s.schema %}
  {% set table = s.identifier %}

  {% if target.name == 'test' %}
    {% set db = db ~ '_test' %}
  {% endif %}

  {{ return(db ~ '.' ~ schema ~ '.' ~ table) }}
{% endmacro %}
