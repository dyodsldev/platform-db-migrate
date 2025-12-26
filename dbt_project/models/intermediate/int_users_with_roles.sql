{{
    config(
        materialized='ephemeral',
        tags=['intermediate', 'users']
    )
}}

WITH users AS (
    SELECT * FROM {{ ref('stg_users') }}
),

roles AS (
    SELECT * FROM {{ ref('stg_roles') }}
),

facilities AS (
    SELECT * FROM {{ ref('int_facilities') }}
),

-- Find which users created/updated which patients
user_patient_ownership AS (
    SELECT DISTINCT
        updated_by AS username,
        owner AS facility_code
    FROM {{ ref('stg_patient_history') }}
    WHERE updated_by IS NOT NULL 
      AND owner IS NOT NULL
),

-- Map users to facilities via patient ownership
user_facilities AS (
    SELECT
        u.username,
        f.id AS facility_id,
        ROW_NUMBER() OVER (PARTITION BY u.username ORDER BY COUNT(*) DESC) AS rank
    FROM user_patient_ownership upo
    JOIN users u ON upo.username = u.username
    LEFT JOIN facilities f ON upo.facility_code = f.mongodb_org_code
        OR upo.facility_code = f.code
    GROUP BY u.username, f.id
),

-- Get primary facility (most common)
primary_facilities AS (
    SELECT 
        username,
        facility_id AS primary_facility_id
    FROM user_facilities
    WHERE rank = 1
),

-- Join users with roles
users_with_roles AS (
    SELECT
        u.*,
        r.role_name AS mongodb_role_name
    FROM users u
    LEFT JOIN roles r ON u.mongodb_role_id = r.mongodb_role_id
),

-- Final mapping
users_mapped AS (
    SELECT
        gen_random_uuid() AS id,
        ROW_NUMBER() OVER (ORDER BY u.mongodb_user_id) AS user_number,
        u.mongodb_user_id,
        
        -- Map to new role
        CASE 
            WHEN LOWER(COALESCE(u.mongodb_role_name, u.mongodb_role_id)) LIKE '%admin%' 
                AND NOT LOWER(COALESCE(u.mongodb_role_name, u.mongodb_role_id)) LIKE '%facility%'
                THEN 1
            WHEN LOWER(COALESCE(u.mongodb_role_name, u.mongodb_role_id)) LIKE '%facility%admin%' 
                THEN 2
            WHEN LOWER(COALESCE(u.mongodb_role_name, u.mongodb_role_id)) LIKE '%doctor%' 
                THEN 4
            WHEN LOWER(COALESCE(u.mongodb_role_name, u.mongodb_role_id)) LIKE '%nurse%' 
                THEN 5
            WHEN LOWER(COALESCE(u.mongodb_role_name, u.mongodb_role_id)) LIKE '%lab%' 
                THEN 6
            WHEN LOWER(COALESCE(u.mongodb_role_name, u.mongodb_role_id)) LIKE '%reception%' 
                THEN 8
            ELSE 7
        END AS role_id,
        
        -- Facility
        pf.primary_facility_id,
        
        -- User details
        u.username,
        u.email,
        u.password_hash,
        u.first_name,
        u.last_name,
        u.title,
        u.phone,
        u.license_number,
        u.specialization,
        u.department,
        u.mfa_enabled,
        u.is_active,
        u.created_at,
        u.updated_at,
        
        -- Audit
        u.mongodb_role_id,
        u.mongodb_role_name
        
    FROM users_with_roles u
    LEFT JOIN primary_facilities pf ON u.username = pf.username
)

SELECT * FROM users_mapped