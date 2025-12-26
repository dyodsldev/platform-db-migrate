{{
    config(
        materialized='table',
        tags=['marts', 'lookups'],
        post_hook=[
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_complication_types_code ON {{ this }} (code)",
            "COMMENT ON TABLE {{ this }} IS 'Complication type reference data'"
        ]
    )
}}

SELECT
    id,
    code,
    name,
    category,
    severity_levels,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM {{ ref('complication_types') }}