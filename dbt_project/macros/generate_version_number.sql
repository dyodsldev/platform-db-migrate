{% macro generate_version_number(patient_id_column, date_column='created_at') %}
    ROW_NUMBER() OVER (
        PARTITION BY {{ patient_id_column }}
        ORDER BY {{ date_column }}
    )
{% endmacro %}