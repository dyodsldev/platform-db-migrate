{{
    config(
        materialized='table',
        tags=['intermediate', 'facilities']
    )
}}

-- PRIMARY: Use facility_mapping as source of truth
WITH facility_mapping AS (
    SELECT
        new_facility_id AS facility_id,
        new_facility_code AS code,
        new_facility_name AS name,
        old_organization_id AS mongodb_org_id,
        old_organization_code AS mongodb_org_code,
        migration_notes
    FROM {{ ref('facility_mapping') }}
),

-- SECONDARY: Find any unmapped owners from patient data
owners_in_data AS (
    SELECT DISTINCT owner
    FROM {{ ref('stg_patient_history') }}
    WHERE owner IS NOT NULL
    
    UNION
    
    SELECT DISTINCT owner
    FROM {{ ref('stg_patient_latest_data') }}
    WHERE owner IS NOT NULL
),

-- Check for unmapped facilities
unmapped_owners AS (
    SELECT 
        o.owner,
        'UNMAPPED_' || ROW_NUMBER() OVER (ORDER BY o.owner) AS temp_facility_id
    FROM owners_in_data o
    LEFT JOIN facility_mapping fm
        ON o.owner = fm.mongodb_org_code
        OR o.owner = fm.mongodb_org_id::text
    WHERE fm.facility_id IS NULL
),

-- Combine mapped and unmapped
final_facilities AS (
    -- Mapped facilities from CSV
    SELECT
        gen_random_uuid() AS id,
        facility_id::text AS facility_id,  -- ← CAST TO TEXT
        code,
        name,
        mongodb_org_id,
        mongodb_org_code,
        'hospital' AS type,
        'LK' AS country_code,
        true AS is_active,
        migration_notes,
        false AS is_unmapped,
        CURRENT_TIMESTAMP AS created_at
    FROM facility_mapping
    
    UNION ALL
    
    -- Unmapped facilities found in data
    SELECT
        gen_random_uuid() AS id,
        temp_facility_id AS facility_id,  -- Already TEXT
        owner AS code,
        'UNMAPPED: ' || owner AS name,
        owner AS mongodb_org_id,
        owner AS mongodb_org_code,
        'hospital' AS type,
        'LK' AS country_code,
        true AS is_active,
        'Auto-generated during migration - needs manual review' AS migration_notes,
        true AS is_unmapped,
        CURRENT_TIMESTAMP AS created_at
    FROM unmapped_owners
)

SELECT * FROM final_facilities