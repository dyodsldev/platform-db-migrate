{{
    config(
        materialized='table',
        tags=['marts', 'lookups'],
        post_hook=[
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_followup_types_code ON {{ this }} (code)",
            "COMMENT ON TABLE {{ this }} IS 'Follow-up type reference data'"
        ]
    )
}}

SELECT
    id,
    facility_id,
    code,
    name,
    category,
    is_baseline,
    typical_interval_days,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM {{ ref('followup_types') }}