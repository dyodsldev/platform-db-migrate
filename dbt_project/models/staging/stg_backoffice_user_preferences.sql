{{
    config(
        materialized='view',
        tags=['staging', 'preferences']
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('medicaldb.backoffice_user_preferences') }}
),

cleaned AS (
    SELECT
        -- IDs
        _id::text AS mongodb_preference_id,
        username,
        
        -- Table: Roles
        table_roles AS table_roles_state,
        table_roles_pagination_range AS table_roles_pagination,
        
        -- Table: Users
        table_users AS table_users_state,
        table_users_sort_column AS table_users_sort_column,
        table_users_pagination_range AS table_users_pagination,
        
        -- Table: Patients
        table_patients AS table_patients_state,
        table_patients_sort_column AS table_patients_sort_column,
        table_patients_sort_direction AS table_patients_sort_direction,
        table_patients_filter_value AS table_patients_filter,
        table_patients_pagination_range AS table_patients_pagination,
        
        -- Table: Patient Records View
        table_view_patient_records_sort_column AS patient_records_sort_column,
        
        -- UI State
        patient_info_panel_expanded AS patient_panel_expanded,
        
        -- Metadata
        to_timestamp(updatedTime / 1000.0) AS updated_at,
        updatedBy AS updated_by,
        updatedFrom AS updated_from,
        updatedCause AS updated_cause,
        updateSequence AS update_sequence
        
    FROM source
)

SELECT * FROM cleaned