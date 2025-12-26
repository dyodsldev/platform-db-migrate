{{
    config(
        materialized='view',
        tags=['staging', 'history']
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('medicaldb.patient_history') }}
),

cleaned AS (
    SELECT
        -- ============================================
        -- IDENTIFIERS
        -- ============================================
        _id::text AS mongodb_history_id,
        code AS patient_code,
        nic AS national_id,
        
        -- ============================================
        -- DEMOGRAPHICS
        -- ============================================
        firstName AS first_name,
        lastName AS last_name,
        gender,
        to_timestamp(dob / 1000.0) AS date_of_birth,
        ageAtDiagnosis AS age_at_diagnosis,
        educationLevel AS education_level,
        
        -- ============================================
        -- CONTACT
        -- ============================================
        contactNumber AS contact_number,
        contactNumber2 AS contact_number_2,
        address,
        email,
        
        -- ============================================
        -- RECORD METADATA
        -- ============================================
        to_timestamp(createdTime / 1000.0) AS record_created_at,
        followUp AS followup_number,
        followupYear AS followup_year,
        to_timestamp(followUpTime / 1000.0) AS follow_up_time,
        to_timestamp(lastFollowupAt / 1000.0) AS last_followup_at,
        baselineIncomplete AS baseline_incomplete,
        followupIncomplete AS followup_incomplete,
        
        -- ============================================
        -- DIAGNOSIS & BASELINE
        -- ============================================
        diagnosis,
        diagnosisExtra AS diagnosis_extra,
        familyHx AS family_history,
        pancreaticDiabetesCause AS pancreatic_diabetes_cause,
        modyType AS mody_type,
        modyGene AS mody_gene,
        
        -- ============================================
        -- C-PEPTIDE & ANTIBODIES
        -- ============================================
        cPeptideDone AS c_peptide_done,
        to_timestamp(cPeptideDoneAt / 1000.0) AS c_peptide_done_at,
        cPeptide AS c_peptide_value,
        cPeptideUnit AS c_peptide_unit,
        gadDone AS gad_done,
        to_timestamp(gadDoneAt / 1000.0) AS gad_done_at,
        gad AS gad_result,
        
        -- ============================================
        -- PHYSICAL MEASUREMENTS
        -- ============================================
        height,
        weight,
        systolicBP AS systolic_bp,
        diastolicBP AS diastolic_bp,
        
        -- ============================================
        -- COMPLICATIONS (Arrays)
        -- ============================================
        ARRAY_REMOVE(ARRAY[
            macrovascularComplications_0,
            macrovascularComplications_1,
            macrovascularComplications_2,
            macrovascularComplications_3,
            macrovascularComplications_4
        ], NULL) AS macrovascular_complications,
        
        retinopathy,
        neuropathy,
        nephropathy,
        noNephropathyFeatures AS no_nephropathy_features,
        diabeticFootDisease AS diabetic_foot_disease,
        
        -- ============================================
        -- OTHER DISEASES (Array - 6 items)
        -- ============================================
        ARRAY_REMOVE(ARRAY[
            otherDiseases_0,
            otherDiseases_1,
            otherDiseases_2,
            otherDiseases_3,
            otherDiseases_4,
            otherDiseases_5
        ], NULL) AS other_diseases,
        
        -- ============================================
        -- CARDIOVASCULAR RISK FACTORS
        -- ============================================
        hypertension,
        hypertensionTargetAchieved AS hypertension_target_achieved,
        dyslipidemia,
        dyslipidemicTargetAchieved AS dyslipidemic_target_achieved,
        smoking,
        
        -- ============================================
        -- KIDNEY FUNCTION
        -- ============================================
        gfr,
        serumCreatinine AS serum_creatinine,
        serumCreatinineUnit AS serum_creatinine_unit,
        serumCreatinineHigh AS serum_creatinine_high,
        microAlbuminurea AS micro_albuminuria,
        macroAlbumiurea AS macro_albuminuria,
        urineAlbuminCreatinRatio AS urine_albumin_creatin_ratio,
        urineAlbuminCreatinineUnit AS urine_albumin_creatinine_unit,
        urinAlbuminCreatinineUnit AS urine_albumin_creatinine_unit_alt,
        
        -- ============================================
        -- DIABETES SPECIFIC
        -- ============================================
        dka,
        dkaFrequency AS dka_frequency,
        episodesOfDKA AS episodes_of_dka,
        hypoglycemiaFrequency AS hypoglycemia_frequency,
        hypoglycemiaSeverity AS hypoglycemia_severity,
        hypoglycemiaAwareness AS hypoglycemia_awareness,
        noOfSevereHypoglycemia AS no_of_severe_hypoglycemia,
        noOfLessSevereHypoglycemia AS no_of_less_severe_hypoglycemia,
        occurenceOfevereHypoglycemia AS occurrence_of_severe_hypoglycemia,
        frequencyOfLessSevereHypoglycemia AS frequency_of_less_severe_hypoglycemia,
        
        -- ============================================
        -- TREATMENT (handles duplicate columns)
        -- ============================================
        currentDmTreatment AS current_dm_treatment,
        insulinRegimen AS insulin_regimen,
        
        -- Use newer column names if available, fallback to older
        COALESCE(typeOfLongActingInsulin, typeOfLongLastingInsulin, typeOfLongLastingInsuline) AS type_of_long_acting_insulin,
        COALESCE(typeOfShortActingInsulin, typeOfShortActiveInsulin, typeOfShortActiveInsuline) AS type_of_short_acting_insulin,
        COALESCE(homeAdjustmentOfInsulin, homeAdjustmentOfInsuline) AS home_adjustment_of_insulin,
        
        dailyInjections AS daily_injections,
        totalDailyDose AS total_daily_dose,
        carbCounting AS carb_counting,
        
        -- ============================================
        -- MONITORING & HBA1C
        -- ============================================
        hbalcValuePercent AS hba1c_value_percent,
        to_timestamp(hba1cDate / 1000.0) AS hba1c_date,
        secondHba1cValuePercent AS second_hba1c_value_percent,
        to_timestamp(secondHba1cDate / 1000.0) AS second_hba1c_date,
        thirdHba1cValuePercent AS third_hba1c_value_percent,
        to_timestamp(thirdHba1cDate / 1000.0) AS third_hba1c_date,
        fourthHba1cValuePercent AS fourth_hba1c_value_percent,
        to_timestamp(fourthHba1cDate / 1000.0) AS fourth_hba1c_date,
        
        glucoseMonitoringMethod AS glucose_monitoring_method,
        smbgFrequency AS smbg_frequency,
        smbgNumberPerWeek AS smbg_number_per_week,
        smbgNumberPerMonth AS smbg_number_per_month,
        
        -- ============================================
        -- EDUCATION & LIFESTYLE
        -- ============================================
        attendingSchool AS attending_school,
        attendingSpecialSchool AS attending_special_school,
        grade,
        appropriateGrade AS appropriate_grade,
        limitingSchool AS limiting_school,
        reasonForNoSchool AS reason_for_no_school,
        highestLevelOfEducation AS highest_level_of_education,
        educationProgram AS education_program,
        diabetesCopingCapabilities AS diabetes_coping_capabilities,
        
        -- ============================================
        -- RESOURCES & SUPPORT (9 items)
        -- ============================================
        donationsRequired AS donations_required,
        ARRAY_REMOVE(ARRAY[
            freeResources_0,
            freeResources_1,
            freeResources_2,
            freeResources_3,
            freeResources_4,
            freeResources_5,
            freeResources_6,
            freeResources_7,
            freeResources_8
        ], NULL) AS free_resources,
        
        -- ============================================
        -- STATUS
        -- ============================================
        status,
        to_timestamp(deceasedDate / 1000.0) AS deceased_date,
        causeOfDeath AS cause_of_death,
        
        -- ============================================
        -- MEDIA
        -- ============================================
        patientLatestPicture AS patient_latest_picture,
        
        -- ============================================
        -- OWNERSHIP & AUDIT
        -- ============================================
        owner,
        updatedBy AS updated_by,
        updatedFrom AS updated_from,
        updatedCause AS updated_cause,
        updateSequence AS update_sequence,
        to_timestamp(updatedTime / 1000.0) AS updated_at,
        to_timestamp(lastUpdatedTime / 1000.0) AS last_updated_at,
        
        -- ============================================
        -- NOTES
        -- ============================================
        specialNotes AS special_notes,
        
        -- ============================================
        -- MISSING FIELDS (for data quality tracking)
        -- ============================================
        ARRAY_REMOVE(ARRAY[
            missingFields_0, missingFields_1, missingFields_2,
            missingFields_3, missingFields_4, missingFields_5,
            missingFields_6, missingFields_7, missingFields_8,
            missingFields_9, missingFields_10, missingFields_11,
            missingFields_12, missingFields_13, missingFields_14,
            missingFields_15, missingFields_16, missingFields_17
        ], NULL) AS missing_fields
        
    FROM source
)

SELECT * FROM cleaned