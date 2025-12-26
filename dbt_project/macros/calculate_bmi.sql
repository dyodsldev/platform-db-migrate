{% macro calculate_bmi(weight_column, height_column) %}
    CASE
        WHEN {{ weight_column }} IS NULL OR {{ height_column }} IS NULL THEN NULL
        WHEN {{ height_column }} = 0 THEN NULL
        ELSE ROUND(
            ({{ weight_column }} / POWER(({{ height_column }} / 100.0), 2))::NUMERIC,
            2
        )
    END
{% endmacro %}