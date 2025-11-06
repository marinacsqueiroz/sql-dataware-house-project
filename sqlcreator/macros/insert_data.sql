{% macro inser_data(file_path, table_path) %}
  {% set path = file_path | replace('\\', '\\\\') %}

  {% set sql %}
    BULK INSERT {{ table_path }}
    FROM '{{ path }}'
    WITH ( 
        FIRSTROW = 2, 
        FIELDTERMINATOR = ',', 
        ROWTERMINATOR = '0x0a', 
        CODEPAGE = '65001', 
        TABLOCK );
  {% endset %}

  {{ log('[INSERT] ' ~ table_path ~ ' <= ' ~ file_path, info=True) }}
  {% do run_query(sql) %}
{% endmacro %}