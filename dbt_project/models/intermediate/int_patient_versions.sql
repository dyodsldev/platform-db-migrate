{{
    config(
        materialized='ephemeral',
        tags=['intermediate', 'patients']
    )
}}

-- Combine patient_history and patient_followups into temporal versions
WITH history AS (
    SELECT
        mongodb_history_id AS source_id,
        'history' AS source_type,
        patient_code,
        record_created_at AS visit_date,
        baseline_incomplete AS is_baseline_incomplete,
        diagnosis,
        weight,
        height,
        systolic_bp,
        diastolic_bp,
        hba1c_value_percent AS hba1c,
        hba1c_date,
        NULL::numeric AS fbs,
        NULL::date AS fbs_date,
        NULL::numeric AS ppbs,
        NULL::date AS ppbs_date,
        retinopathy,
        neuropathy,
        nephropathy,
        macrovascular_complications,
        current_dm_treatment,
        insulin_regimen,
        education_level,
        special_notes AS clinical_notes,
        updated_at AS created_at,
        updated_by AS created_by_username,
        owner AS facility_code
    FROM {{ ref('stg_patient_history') }}
),

followups AS (
    SELECT
        mongodb_followup_id AS source_id,
        'followup' AS source_type,
        patient_code,
        followup_date AS visit_date,
        baseline_incomplete AS is_baseline_incomplete,
        diagnosis,
        weight,
        height,
        systolic_bp,
        diastolic_bp,
        hba1c_value_percent AS hba1c,
        hba1c_date,
        NULL::numeric AS fbs,
        NULL::date AS fbs_date,
        NULL::numeric AS ppbs,
        NULL::date AS ppbs_date,
        retinopathy,
        neuropathy,
        nephropathy,
        macrovascular_complications,
        current_dm_treatment,
        insulin_regimen,
        education_level,
        special_notes AS clinical_notes,
        updated_at AS created_at,
        updated_by AS created_by_username,
        NULL AS facility_code
    FROM {{ ref('stg_patient_followups') }}
),

combined AS (
    SELECT * FROM history
    UNION ALL
    SELECT * FROM followups
),

-- Get unique patients
patients AS (
    SELECT DISTINCT
        gen_random_uuid() AS patient_id,
        patient_code,
        first_name,
        last_name,
        date_of_birth,
        gender
    FROM {{ ref('stg_patients') }}
),

-- Get facilities
facilities AS (
    SELECT 
        id AS facility_id,
        code AS facility_code,
        mongodb_organization_code
    FROM {{ ref('int_facilities') }}
),

-- Get users
users AS (
    SELECT 
        id AS user_id,
        username
    FROM {{ ref('int_users_with_roles') }}
),

-- Map patients to facilities via owner field
patients_with_facility AS (
    SELECT
        p.patient_id,
        p.patient_code,
        COALESCE(f.facility_id, '1')::text AS facility_id
    FROM patients p
    LEFT JOIN (
        SELECT DISTINCT 
            patient_code,
            owner
        FROM {{ ref('stg_patient_history') }}
        WHERE owner IS NOT NULL
    ) ph ON p.patient_code = ph.patient_code
    LEFT JOIN facilities f 
        ON ph.owner = f.facility_code
        OR ph.owner = f.mongodb_organization_code
),

-- Create versions with temporal data
versions_with_ids AS (
    SELECT
        gen_random_uuid() AS version_id,
        p.patient_id,
        p.facility_id,
        
        -- Use your macro for version number
        {{ generate_version_number('p.patient_id', 'c.visit_date') }} AS version_number,
        
        -- Temporal validity
        c.visit_date AS valid_from,
        LEAD(c.visit_date) OVER (
            PARTITION BY p.patient_id 
            ORDER BY c.visit_date
        ) AS valid_to,
        
        -- Visit info
        NOT c.is_baseline_incomplete AS is_baseline,
        c.visit_date,
        
        -- Medical data
        c.diagnosis AS diagnosis_code,
        c.hba1c AS hba1c_latest,
        c.hba1c_date,
        c.fbs AS fbs_latest,
        c.fbs_date,
        c.ppbs AS ppbs_latest,
        c.ppbs_date,
        c.weight,
        c.height,
        
        -- Use your BMI macro
        {{ calculate_bmi('c.weight', 'c.height') }} AS bmi,
        
        c.systolic_bp AS bp_systolic,
        c.diastolic_bp AS bp_diastolic,
        
        -- Convert integer codes to booleans
        CASE WHEN c.retinopathy > 0 THEN true ELSE false END AS has_retinopathy,
        CASE WHEN c.neuropathy > 0 THEN true ELSE false END AS has_neuropathy,
        CASE WHEN c.nephropathy > 0 THEN true ELSE false END AS has_nephropathy,
        CASE WHEN ARRAY_LENGTH(c.macrovascular_complications, 1) > 0 THEN true ELSE false END AS has_cvd,
        
        -- Treatment
        CASE WHEN c.insulin_regimen IS NOT NULL AND c.insulin_regimen > 0 THEN true ELSE false END AS on_insulin,
        CASE WHEN c.current_dm_treatment IS NOT NULL AND c.current_dm_treatment > 0 THEN true ELSE false END AS on_oral_meds,
        
        c.education_level AS education_level_code,
        c.clinical_notes,
        
        c.source_type,
        c.source_id,
        c.created_at,
        u.user_id AS created_by_user_id,
        c.created_by_username
        
    FROM combined c
    JOIN patients_with_facility p ON c.patient_code = p.patient_code
    LEFT JOIN users u ON c.created_by_username = u.username
)

SELECT * FROM versions_with_ids