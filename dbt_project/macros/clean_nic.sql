{% macro clean_nic(nic_column) %}
    CASE
        WHEN {{ nic_column }} IS NULL THEN NULL
        WHEN LENGTH(TRIM({{ nic_column }})) = 0 THEN NULL
        ELSE REGEXP_REPLACE(UPPER(TRIM({{ nic_column }})), '[^A-Z0-9]', '', 'g')
    END
{% endmacro %}