{{
    config(
        materialized='view',
        tags=['staging', 'roles']
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('medicaldb.roles') }}
),

cleaned AS (
    SELECT
        -- IDs
        _id::text AS mongodb_role_id,
        
        -- Role Info
        name AS role_name,
        
        -- Privileges (combine array columns into JSONB array)
        ARRAY_REMOVE(
            ARRAY[
                privileges_0,
                privileges_1,
                privileges_2,
                privileges_3
            ],
            NULL
        ) AS privileges_array,
        
        -- Or as JSONB for flexibility
        JSONB_BUILD_ARRAY(
            privileges_0,
            privileges_1,
            privileges_2,
            privileges_3
        ) - 'null' AS privileges_jsonb,
        
        -- Metadata
        to_timestamp(updatedTime / 1000.0) AS updated_at,
        updatedBy AS updated_by,
        updatedFrom AS updated_from,
        updatedCause AS updated_cause,
        updateSequence AS update_sequence,
        
        -- Defaults
        NULL::timestamp AS created_at
        
    FROM source
)

SELECT * FROM cleaned