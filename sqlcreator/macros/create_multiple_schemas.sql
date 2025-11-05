{# 
This macro iterates through a list of schema names and ensures that each one exists.

How it works:
- It receives a list of schema names (`schema_list`) as an argument.
- For each schema in the list:
    - It logs a message indicating which schema is being processed.
    - It calls the `create_schema_if_not_exists` macro to create the schema 
    only if it does not already exist.
- This makes it easy to create multiple schemas in a single dbt operation.

Notes:
- Depends on the `create_schema_if_not_exists` macro (which must be defined separately).
- Safe to run multiple times — existing schemas are not recreated.
- Useful for initializing environments with several layers (e.g., bronze, silver, gold). 
#}


{% macro create_multiple_schemas(schema_list) %}
  {% for schema in schema_list %}

    {{ log("Creating schema if not exists: " ~ schema, info=True) }}
    {{ create_schema_if_not_exists(schema_name=schema) }}

  {% endfor %}
{% endmacro %}