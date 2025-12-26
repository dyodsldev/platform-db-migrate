{{
    config(
        materialized='table',
        tags=['marts', 'lookups'],
        post_hook=[
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_units_code ON {{ this }} (code)",
            "COMMENT ON TABLE {{ this }} IS 'Measurement unit reference data'"
        ]
    )
}}

SELECT
    id,
    facility_id,
    code,
    name,
    symbol,
    base_unit_id,
    conversion_factor,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM {{ ref('units') }}