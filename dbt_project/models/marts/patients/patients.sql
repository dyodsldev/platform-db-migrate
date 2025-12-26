{{
    config(
        materialized='table',
        tags=['marts', 'patients'],
        post_hook=[
			"ALTER TABLE {{ this }} ADD PRIMARY KEY (id)",
            "CREATE INDEX IF NOT EXISTS idx_patients_code ON {{ this }} (code)",
            "CREATE INDEX IF NOT EXISTS idx_patients_facility ON {{ this }} (current_facility_id)",
            "CREATE INDEX IF NOT EXISTS idx_patients_status ON {{ this }} (status)",
            "CREATE INDEX IF NOT EXISTS idx_patients_dob ON {{ this }} (date_of_birth)",
            "CREATE INDEX IF NOT EXISTS idx_patients_is_active ON {{ this }} (is_active)"
        ]
    )
}}

WITH staging_patients AS (
    SELECT * FROM {{ ref('stg_patients') }}
),

facilities AS (
    SELECT 
        id AS facility_id,
        code AS facility_code,
        mongodb_org_code
    FROM {{ ref('facilities') }}  -- Use marts.facilities, not int_facilities
),

users AS (
    SELECT 
        id AS user_id,
        mongodb_user_id,
        username
    FROM {{ ref('users') }}  -- Use marts.users, not int_users_with_roles
),

-- Map patients to facilities via owner field
patients_with_facility AS (
    SELECT
        sp.*,
        COALESCE(f.facility_id, 
            (SELECT id FROM {{ ref('facilities') }} WHERE is_active = true ORDER BY created_at LIMIT 1)
        ) AS facility_id
    FROM staging_patients sp
    LEFT JOIN facilities f 
        ON sp.owner = f.facility_code 
        OR sp.owner = f.mongodb_org_code
),

-- Map to users via username
patients_full AS (
    SELECT
        gen_random_uuid() AS id,  -- ← Stable UUID, not ROW_NUMBER
        p.mongodb_patient_id,
        p.patient_code AS code,
        p.facility_id AS current_facility_id,
        p.facility_id AS registered_facility_id,  -- Same on initial registration
        
        -- Demographics
        p.first_name,
        p.last_name,
        p.gender,
        p.date_of_birth,
        p.age_at_diagnosis,
        
        -- Identification
        p.national_id AS nic,
        NULL::text AS passport_number,  -- Not in MongoDB
        
        -- Contact
        p.contact_number,
        p.contact_number_2,
        p.email,
        p.address,
        NULL::text AS city,  -- Not in MongoDB
        NULL::text AS postal_code,  -- Not in MongoDB
        
        -- Emergency Contact (not in MongoDB)
        NULL::text AS emergency_contact_name,
        NULL::text AS emergency_contact_phone,
        NULL::text AS emergency_contact_relationship,
        
        -- Ownership & Status
        u_owner.user_id AS owner_user_id,
        p.status,
        CASE WHEN p.status = 0 THEN false ELSE true END AS is_active,
        p.deceased_date,
        
        -- Media
        p.patient_latest_picture AS photo_url,
        
        -- Notes
        p.special_notes AS notes,
        
        -- Metadata
        p.updated_at AS created_at,  -- Best approximation
        u_created.user_id AS created_by_user_id,
        p.updated_at
        
    FROM patients_with_facility p
    LEFT JOIN users u_owner ON p.owner = u_owner.username
    LEFT JOIN users u_created ON p.updated_by = u_created.username
)

SELECT * FROM patients_full