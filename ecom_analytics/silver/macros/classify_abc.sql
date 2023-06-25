{% macro classify_abc(column_name) %}

CASE
    WHEN "{{column_name}}" <= 50 THEN 'A (50%)'
    WHEN "{{column_name}}" <= 90 THEN 'B (40%)'
    ELSE 'C (10%)'
END

{% endmacro %}