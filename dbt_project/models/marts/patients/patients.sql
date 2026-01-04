{{
    config(
        materialized='table',
        tags=['marts', 'patients'],
        post_hook=[
            "ALTER TABLE {{ this }} ADD PRIMARY KEY (id)",
            "CREATE INDEX IF NOT EXISTS idx_patients_code ON {{ this }} (code)",
            "CREATE INDEX IF NOT EXISTS idx_patients_facility ON {{ this }} (current_facility_id)",
            "CREATE INDEX IF NOT EXISTS idx_patients_owner ON {{ this }} (owner_user_id)",
            "CREATE INDEX IF NOT EXISTS idx_patients_dob ON {{ this }} (date_of_birth)",
            "CREATE INDEX IF NOT EXISTS idx_patients_diagnosis ON {{ this }} (diagnosis_type_id)",
            "CREATE INDEX IF NOT EXISTS idx_patients_active ON {{ this }} (is_active) WHERE is_active = true",
            "CREATE INDEX IF NOT EXISTS idx_patients_deceased ON {{ this }} (is_deceased) WHERE is_deceased = true",
            "CREATE INDEX IF NOT EXISTS idx_patients_name ON {{ this }} (LOWER(last_name), LOWER(first_name))"
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
    FROM {{ ref('facilities') }}
),

users AS (
    SELECT 
        id AS user_id,
        mongodb_user_id,
        username
    FROM {{ ref('users') }}
),

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

patients_full AS (
    SELECT
        gen_random_uuid() AS id,
        p.mongodb_patient_id,
        p.patient_code AS code,
        
        -- Demographics
        COALESCE(p.first_name, u_owner.username, 'Unknown') AS first_name,
        COALESCE(p.last_name, 'Patient') AS last_name,
        p.gender,
        p.date_of_birth,
        
        -- Identification
        p.national_id AS nic,
        NULL::text AS passport_number,
        
        -- Contact
        CASE 
            WHEN p.contact_number IS NULL THEN NULL
            ELSE p.contact_number::TEXT
        END AS phone,

        p.email,
        p.address,
        NULL::text AS city,
        NULL::text AS province,  -- ⭐ Added
        
        -- ⭐ NEW: Demographics lookup references
        NULL::integer AS occupation_type_id,  -- TODO: Map from MongoDB if available
        NULL::integer AS education_level_id,   -- TODO: Map from MongoDB if available
        NULL::integer AS marital_status_id,    -- TODO: Map from MongoDB if available
        
        -- ⭐ NEW: Clinical data
        NULL::integer AS diagnosis_type_id,    -- TODO: Map from MongoDB diagnosis
        NULL::date AS diagnosis_date,          -- TODO: Extract from MongoDB
        
        -- Facility & Ownership
        p.facility_id AS current_facility_id,
        u_owner.user_id AS owner_user_id,
        
        -- Status
        CASE WHEN p.status = 0 THEN false ELSE true END AS is_active,
        CASE WHEN p.deceased_date IS NOT NULL THEN true ELSE false END AS is_deceased,  -- ⭐ Added
        p.deceased_date,
        
        -- Notes
        p.special_notes AS notes,
        
        -- Metadata
        p.updated_at AS created_at,
        p.updated_at
        
    FROM patients_with_facility p
    LEFT JOIN users u_owner ON p.owner = u_owner.username
)

SELECT * FROM patients_full