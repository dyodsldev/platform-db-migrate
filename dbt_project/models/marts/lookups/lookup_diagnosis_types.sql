{{
    config(
        materialized='table',
        tags=['marts', 'lookups'],
        post_hook=[
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_diagnosis_types_code ON {{ this }} (code)",
            "CREATE INDEX IF NOT EXISTS idx_diagnosis_types_category ON {{ this }} (category)",
            "COMMENT ON TABLE {{ this }} IS 'Diagnosis type reference data'"
        ]
    )
}}

SELECT
    id,
    code,
    name,
    category,
    description,
    
    -- Add metadata
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
    
FROM {{ ref('diagnosis_types') }}