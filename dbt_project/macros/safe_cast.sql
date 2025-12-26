{% macro safe_cast(column, data_type, default_value='NULL') %}
    CASE
        WHEN {{ column }} IS NULL THEN {{ default_value }}
        WHEN TRIM({{ column }}::text) = '' THEN {{ default_value }}
        ELSE {{ column }}::{{ data_type }}
    END
{% endmacro %}