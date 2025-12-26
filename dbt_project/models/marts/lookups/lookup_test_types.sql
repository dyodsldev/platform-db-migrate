{{
    config(
        materialized='table',
        tags=['marts', 'lookups'],
        post_hook=[
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_test_types_code ON {{ this }} (code)",
            "COMMENT ON TABLE {{ this }} IS 'Test type reference data'"
        ]
    )
}}

SELECT
    id,
    facility_id,
    code,
    name,
    category,
    default_unit_id,
    normal_range_min,
    normal_range_max,
    is_routine,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM {{ ref('test_types') }}