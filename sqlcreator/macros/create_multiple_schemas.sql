{% macro create_multiple_schemas(schema_list) %}
  {% for schema in schema_list %}

    {{ log("Creating schema if not exists: " ~ schema, info=True) }}
    {{ create_schema_if_not_exists(schema_name=schema) }}
    
  {% endfor %}
{% endmacro %}