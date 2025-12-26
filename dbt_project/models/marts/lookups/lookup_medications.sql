{{
    config(
        materialized='table',
        tags=['marts', 'lookups'],
        post_hook=[
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_medications_code ON {{ this }} (code)",
            "COMMENT ON TABLE {{ this }} IS 'Medication reference data'"
        ]
    )
}}

SELECT
    id,
    facility_id,
    code,
    name,
    generic_name,
    brand_name,
    drug_class,
    category,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM {{ ref('medications') }}