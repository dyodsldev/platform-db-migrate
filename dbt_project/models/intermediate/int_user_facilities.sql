{{
    config(
        materialized='ephemeral',
        tags=['intermediate', 'user_facilities']
    )
}}

-- Create M:M user-facility mapping
WITH users AS (
    SELECT * FROM {{ ref('int_users_with_roles') }}
),

user_facility_mapping AS (
    SELECT
        gen_random_uuid() AS id,
        u.id AS user_id,
        u.primary_facility_id AS facility_id,  -- ← FIXED: Removed COALESCE
        
        CASE 
            WHEN u.primary_facility_id IS NOT NULL THEN true 
            ELSE false 
        END AS is_primary,
        
        true AS can_manage_patients,
        
        CASE 
            WHEN u.role_id IN (1, 2, 3, 7) THEN true
            ELSE false 
        END AS can_view_all_patients,
        
        CASE
            WHEN u.role_id IN (1, 2, 4) THEN true
            ELSE false
        END AS can_transfer_patients,
        
        u.created_at AS assignment_start_date,
        true AS is_active,
        CURRENT_TIMESTAMP AS assigned_at
        
    FROM users u
    WHERE u.primary_facility_id IS NOT NULL  -- ← Only include users with facilities
)

SELECT * FROM user_facility_mapping