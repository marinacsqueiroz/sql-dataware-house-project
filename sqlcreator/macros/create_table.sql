{% macro create_table(table_name, columns, schema=none, database=target.database, execute_ddl=true) -%}
  {# columns pode ser dict {"col":"TIPO"} ou string JSON #}
  {% if columns is string %}
    {% set columns = columns | fromjson %}
  {% endif %}
  {% if not (columns is mapping) %}
    {%- do exceptions.raise_compiler_error("O parâmetro 'columns' deve ser um dicionário {coluna: tipo}.") -%}
  {% endif %}

  {# usa schema passado; se vazio, cai no target.schema #}
  {% if schema is none or schema|length == 0 %}
    {% set schema = target.schema %}
  {% endif %}

  {# relation com database/schema/identifier corretos #}
  {% set rel = api.Relation.create(
      database=database,
      schema=schema,
      identifier=table_name
  ) %}

  {# skip se já existir #}
  {% set existing = adapter.get_relation(
      database=database,
      schema=schema,
      identifier=table_name
  ) %}
  {% if existing is not none %}
    {{ log("[SKIP] Tabela já existe: " ~ rel, info=True) }}
    {{ return("SKIP") }}
  {% endif %}

  {# colunas #}
  {% set col_defs = [] %}
  {% for col, tipo in columns.items() %}
    {% do col_defs.append(adapter.quote(col) ~ " " ~ tipo) %}
  {% endfor %}

  {# DDL #}
  {% set ddl -%}
    create table {{ rel }} (
      {{ col_defs | join(",\n      ") }}
    )
  {%- endset %}

  {{ log("[CREATE] " ~ ddl, info=True) }}
  {% if execute_ddl %}{% do run_query(ddl) %}{% endif %}
  {{ return(ddl) }}
{%- endmacro %}
