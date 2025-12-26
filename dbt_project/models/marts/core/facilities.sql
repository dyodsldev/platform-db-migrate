{{
    config(
        materialized='table',
        tags=['marts', 'core'],
        post_hook=[
            "ALTER TABLE {{ this }} ADD PRIMARY KEY (id)",
            "CREATE INDEX IF NOT EXISTS idx_facilities_code ON {{ this }} (code)",
            "CREATE INDEX IF NOT EXISTS idx_facilities_is_active ON {{ this }} (is_active)",
            "CREATE INDEX IF NOT EXISTS idx_facilities_is_unmapped ON {{ this }} (is_unmapped)"
        ]
    )
}}

-- NEW TABLE: Facilities
SELECT
    id,  -- ← Use the UUID
    facility_id AS legacy_facility_id,  -- ← Keep original as reference
    code,
    name,
    type,
    
    -- Location (NULL for now, can be updated later)
    NULL::text AS address,
    NULL::text AS city,
    NULL::text AS district,
    NULL::text AS province,
    'Sri Lanka' AS country,
    NULL::text AS postal_code,
    
    -- Contact (NULL for now)
    NULL::text AS phone,
    NULL::text AS email,
    NULL::text AS website,
    
    -- Capabilities (defaults)
    true AS has_lab,
    true AS has_pharmacy,
    false AS has_imaging,
    
    -- MongoDB reference
    mongodb_org_id,
    mongodb_org_code,
    
    -- Status
    is_active,
    is_unmapped,  -- ← Flag for review
    NULL::date AS opened_date,
    
    -- Metadata
    migration_notes,
    created_at,
    CURRENT_TIMESTAMP AS updated_at
    
FROM {{ ref('int_facilities') }}