{{
    config(
        materialized='table',
        tags=['marts', 'core'],
        post_hook=[
            "ALTER TABLE {{ this }} ADD PRIMARY KEY (id)",
            "CREATE INDEX IF NOT EXISTS idx_users_username ON {{ this }} (username)",
            "CREATE INDEX IF NOT EXISTS idx_users_email ON {{ this }} (email) WHERE email IS NOT NULL",
            "CREATE INDEX IF NOT EXISTS idx_users_role_id ON {{ this }} (role_id)",
            "CREATE INDEX IF NOT EXISTS idx_users_facility ON {{ this }} (primary_facility_id) WHERE primary_facility_id IS NOT NULL",
            "CREATE INDEX IF NOT EXISTS idx_users_is_active ON {{ this }} (is_active)"
        ]
    )
}}

SELECT
    id,
    mongodb_user_id,
    username,
    password_hash,
    COALESCE(first_name, username, 'User') AS first_name,
    COALESCE(last_name, 'User') AS last_name,
    role_id,
    primary_facility_id,  -- ← Can be NULL
    mfa_enabled,
    is_active,
    created_at,
    updated_at,
    
    -- NULL fields (not in MongoDB)
    NULL::text AS email,
    NULL::text AS title,
    NULL::text AS phone,
    NULL::text AS license_number,
    NULL::text AS specialization,
    NULL::text AS department,
    NULL::text AS emergency_contact,
    NULL::text AS emergency_phone,
    NULL::text AS mfa_secret,
    NULL::date AS activation_date,
    NULL::timestamp AS last_login_at,
    NULL::int AS failed_login_attempts,
    NULL::timestamp AS locked_until
    
FROM {{ ref('int_users_with_roles') }}