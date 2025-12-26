{{
    config(
        materialized='table',
        tags=['marts', 'core', 'relationships'],
        post_hook=[
            "CREATE INDEX IF NOT EXISTS idx_user_facilities_user ON {{ this }} (user_id)",
            "CREATE INDEX IF NOT EXISTS idx_user_facilities_facility ON {{ this }} (facility_id)",
            "CREATE INDEX IF NOT EXISTS idx_user_facilities_is_primary ON {{ this }} (user_id, is_primary) WHERE is_primary = true",
            "CREATE INDEX IF NOT EXISTS idx_user_facilities_active ON {{ this }} (is_active)"
        ]
    )
}}

-- M:M mapping between users and facilities
SELECT
    id,  -- ← ADD THIS: Primary key from int_user_facilities
    user_id,
    facility_id,
    
    -- Permissions
    is_primary,
    can_manage_patients,
    can_view_all_patients,
    can_transfer_patients,
    
    -- Temporal tracking
    assignment_start_date,
    NULL::date AS assignment_end_date,  -- For future: when assignment ends
    
    -- Status
    is_active,
    assigned_at
    
FROM {{ ref('int_user_facilities') }}