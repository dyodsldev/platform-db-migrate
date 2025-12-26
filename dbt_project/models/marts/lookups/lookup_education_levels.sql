{{
    config(
        materialized='table',
        tags=['marts', 'lookups'],
        post_hook=[
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_education_levels_code ON {{ this }} (code)",
            "COMMENT ON TABLE {{ this }} IS 'Education level reference data'"
        ]
    )
}}

SELECT
    id,
    code,
    name,
    sort_order,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM {{ ref('education_levels') }}