{{
    config(
        materialized='ephemeral',
        tags=['intermediate', 'user_facilities']
    )
}}

-- Create M:M user-facility mapping
-- Every user gets assigned to their primary facility (or default if none)
WITH users AS (
    SELECT * FROM {{ ref('int_users_with_roles') }}
),

user_facility_mapping AS (
    SELECT
        gen_random_uuid() AS id,
        u.id AS user_id,  -- ← Use the UUID 'id' from int_users_with_roles
        COALESCE(u.primary_facility_id, '1') AS facility_id,  -- ← Default to facility '1' if NULL
        CASE 
            WHEN u.primary_facility_id IS NOT NULL THEN true 
            ELSE false 
        END AS is_primary,  -- ← Only primary if they have an actual facility
        true AS can_manage_patients,
        
        -- Admin roles can view all patients in facility
        CASE 
            WHEN u.role_id IN (1, 2, 3, 7) THEN true  -- ← Kept role 3
            ELSE false 
        END AS can_view_all_patients,
        
        -- Admins and doctors can transfer patients
        CASE
            WHEN u.role_id IN (1, 2, 4) THEN true
            ELSE false
        END AS can_transfer_patients,
        
        u.created_at AS assignment_start_date,
        true AS is_active,
        CURRENT_TIMESTAMP AS assigned_at
        
    FROM users u
    -- ← Removed WHERE clause - now includes ALL users
)

SELECT * FROM user_facility_mapping