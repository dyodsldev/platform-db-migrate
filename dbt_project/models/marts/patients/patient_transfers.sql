{{
    config(
        materialized='table',
        tags=['marts', 'patients', 'transfers'],
        post_hook=[
            "CREATE INDEX IF NOT EXISTS idx_patient_transfers_patient ON {{ this }} (patient_id)",
            "CREATE INDEX IF NOT EXISTS idx_patient_transfers_from_facility ON {{ this }} (from_facility_id)",
            "CREATE INDEX IF NOT EXISTS idx_patient_transfers_to_facility ON {{ this }} (to_facility_id)",
            "CREATE INDEX IF NOT EXISTS idx_patient_transfers_status ON {{ this }} (status)",
            "CREATE INDEX IF NOT EXISTS idx_patient_transfers_requested_at ON {{ this }} (requested_at)",
            "ALTER TABLE {{ this }} ADD CONSTRAINT fk_transfers_patient FOREIGN KEY (patient_id) REFERENCES {{ ref('patients') }}(id)",
            "ALTER TABLE {{ this }} ADD CONSTRAINT fk_transfers_from_facility FOREIGN KEY (from_facility_id) REFERENCES {{ ref('facilities') }}(id)",
            "ALTER TABLE {{ this }} ADD CONSTRAINT fk_transfers_to_facility FOREIGN KEY (to_facility_id) REFERENCES {{ ref('facilities') }}(id)",
            "ALTER TABLE {{ this }} ADD CONSTRAINT fk_transfers_from_doctor FOREIGN KEY (from_doctor_id) REFERENCES {{ ref('users') }}(id)",
            "ALTER TABLE {{ this }} ADD CONSTRAINT fk_transfers_to_doctor FOREIGN KEY (to_doctor_id) REFERENCES {{ ref('users') }}(id)",
            "ALTER TABLE {{ this }} ADD CONSTRAINT fk_transfers_requested_by FOREIGN KEY (requested_by_user_id) REFERENCES {{ ref('users') }}(id)",
            "COMMENT ON TABLE {{ this }} IS 'Patient transfers between facilities - schema ready for future implementation'"
        ]
    )
}}

-- Patient transfer tracking (empty table, schema ready)
-- Will be populated when transfer functionality is implemented
SELECT
    -- IDs (UUID to match rest of schema)
    NULL::uuid AS id,
    NULL::uuid AS patient_id,
    NULL::uuid AS from_facility_id,
    NULL::uuid AS to_facility_id,
    NULL::uuid AS from_doctor_id,
    NULL::uuid AS to_doctor_id,
    
    -- Transfer details
    NULL::text AS transfer_type,  -- 'permanent', 'temporary', 'consultation', 'emergency'
    NULL::text AS reason,
    NULL::text AS clinical_summary,
    NULL::text AS urgency_level,  -- 'routine', 'urgent', 'emergency'
    NULL::text AS transport_required,  -- 'ambulance', 'patient', 'none'
    NULL::text AS medical_equipment_needed,
    NULL::text AS documents_url,  -- Link to transfer documents
    
    -- Status tracking
    NULL::text AS status,  -- 'pending', 'accepted', 'rejected', 'in_transit', 'completed', 'cancelled'
    NULL::timestamp AS requested_at,
    NULL::timestamp AS accepted_at,
    NULL::timestamp AS rejected_at,
    NULL::timestamp AS in_transit_at,
    NULL::timestamp AS completed_at,
    NULL::timestamp AS cancelled_at,
    NULL::timestamp AS transferred_at,  -- Actual transfer time
    
    -- Users involved
    NULL::uuid AS requested_by_user_id,
    NULL::uuid AS accepted_by_user_id,
    NULL::uuid AS completed_by_user_id,
    
    -- Rejection/cancellation tracking
    NULL::text AS rejection_reason,
    NULL::text AS cancellation_reason,
    
    -- Notes
    NULL::text AS notes,
    
    -- Metadata
    NULL::timestamp AS created_at,
    NULL::timestamp AS updated_at
    
WHERE FALSE  -- Empty table, no rows