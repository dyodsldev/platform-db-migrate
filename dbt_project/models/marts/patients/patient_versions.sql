{{
    config(
        materialized='table',
        tags=['marts', 'patients', 'temporal'],
        post_hook=[
            "CREATE INDEX IF NOT EXISTS idx_patient_versions_patient ON {{ this }} (patient_id, version_number)",
            "CREATE INDEX IF NOT EXISTS idx_patient_versions_valid_from ON {{ this }} (valid_from)",
            "CREATE INDEX IF NOT EXISTS idx_patient_versions_valid_to ON {{ this }} (valid_to)",
            "CREATE INDEX IF NOT EXISTS idx_patient_versions_temporal ON {{ this }} (patient_id, valid_from, valid_to)",
            "CREATE INDEX IF NOT EXISTS idx_patient_versions_baseline ON {{ this }} (patient_id, is_baseline) WHERE is_baseline = true",
            "CREATE INDEX IF NOT EXISTS idx_patient_versions_facility ON {{ this }} (facility_id)",
            "CREATE INDEX IF NOT EXISTS idx_patient_versions_visit_date ON {{ this }} (visit_date)"
        ]
    )
}}

-- Temporal patient history table
-- Each row represents a snapshot of patient state at a point in time
SELECT
    version_id as id,
    patient_id,
    facility_id,
    version_number,
    
    -- Temporal validity
    valid_from,
    valid_to,
    CASE WHEN valid_to IS NULL THEN true ELSE false END AS is_current,  -- ← Add this!
    
    -- Visit info
    is_baseline,
    visit_date,
    
    -- Diagnosis
    diagnosis_code,  -- ← Fixed from diagnosis_id
    
    -- Lab results
    hba1c_latest,
    hba1c_date,
    fbs_latest,
    fbs_date,
    ppbs_latest,
    ppbs_date,
    
    -- Physical measurements
    weight,
    height,
    bmi,
    bp_systolic,
    bp_diastolic,
    
    -- Complications
    has_retinopathy,
    has_neuropathy,
    has_nephropathy,
    has_cvd,
    
    -- Treatment
    on_insulin,
    on_oral_meds,
    
    -- Education
    education_level_code,  -- ← Fixed from education_level_id
    
    -- Extended data (JSONB for flexibility)
    NULL::jsonb AS lab_results_extended,
    NULL::jsonb AS medications_detail,
    NULL::jsonb AS complications_detail,
    NULL::jsonb AS vitals_extended,
    
    -- Notes
    clinical_notes,
    
    -- Audit trail
    source_type,  -- ← Added: 'history' or 'followup'
    source_id,    -- ← Added: Original MongoDB record ID
    created_at,
    created_by_user_id,
    created_by_username  -- ← Added: Useful for debugging
    
FROM {{ ref('int_patient_versions') }}