{{
    config(
        materialized='view',
        tags=['staging', 'users']
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('medicaldb.users') }}
),

cleaned AS (
    SELECT
        -- IDs
        _id::text AS mongodb_user_id,
        
        -- Authentication
        username,
        password AS password_hash,
        
        -- Personal Info
        firstName AS first_name,
        lastName AS last_name,
        
        -- Role (MongoDB role reference)
        role AS mongodb_role_id,
        
        -- Metadata
        to_timestamp(updatedTime / 1000.0) AS updated_at,
        updatedBy AS updated_by,
        updatedFrom AS updated_from,
        updatedCause AS updated_cause,
        updateSequence AS update_sequence,
        
        -- Defaults for fields not in MongoDB
        NULL AS email,
        NULL AS phone,
        NULL AS title,
        NULL AS license_number,
        NULL AS specialization,
        NULL AS department,
        NULL AS mongodb_organization_id,
        false AS mfa_enabled,
        true AS is_active,
        NULL::timestamp AS created_at
        
    FROM source
)

SELECT * FROM cleaned