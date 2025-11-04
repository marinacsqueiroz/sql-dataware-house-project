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
