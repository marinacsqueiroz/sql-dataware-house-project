{# 
This macro creates a SQL Server schema only if it does not already exist.

How it works:
- It builds a SQL statement that checks the system catalog (sys.schemas)
  to see if a schema with the given name already exists.
- If the schema is missing, it executes a CREATE SCHEMA command,
  assigning ownership to the 'dbo' user.
- The macro then logs the operation and runs the query through dbt’s `run_query()`.

Notes:
- This macro is specific to SQL Server (T-SQL syntax).
- It is idempotent: running it multiple times will not recreate existing schemas.
#}

{% macro create_schema_if_not_exists(schema_name) %}
    {% set sql %}
        IF NOT EXISTS (
            SELECT * FROM sys.schemas WHERE name = '{{ schema_name }}'
        )
        BEGIN
            EXEC('CREATE SCHEMA {{ schema_name }} AUTHORIZATION dbo;')
        END
    {% endset %}

    {{ log("Creating schema if not exists: " ~ schema_name, info=True) }}
    {{ run_query(sql) }}
{% endmacro %}
