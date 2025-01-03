CREATE OR REPLACE SECURE VIEW healthcare_analytics.gold.unified_patient_profile AS
SELECT
    -- Patient Demographic Information
    dp.patient_id,
    dp.first_name,
    dp.last_name,
    dp.date_of_birth,
    dp.gender,
    dp.race,
    dp.ethnicity,
    dp.address,
    dp.city,
    dp.state,
    dp.zip_code,
    dp.insurance_type,
    -- Provider Information
    prov.provider_name AS primary_care_provider_name,
    prov.specialty AS primary_care_provider_specialty,
    -- Facility Information
    fac.facility_name AS provider_facility_name,
    fac.facility_address AS provider_facility_address,
    fac.facility_state AS provider_facility_state,
    fac.facility_zipcode AS provider_facility_zipcode,
    -- Care Gaps
    cg.gap_type AS care_gap_type,
    cg.status AS care_gap_status,
    cg.recommended_action AS care_gap_action,
    cg.identified_date AS care_gap_identified_date,
    -- Claims Information
    cl.claim_id,
    cl.service_date AS claim_service_date,
    cl.claim_date AS claim_submission_date,
    cl.claim_amount,
    cl.claim_status AS claim_status,
    cl.diagnosis_codes AS claim_diagnosis_codes,
    cl.procedure_codes AS claim_procedure_codes,
    -- Medical Events Information
    me.event_id AS medical_event_id,
    me.event_date AS medical_event_date,
    me.event_type AS medical_event_type,
    me.diagnosis_code AS medical_event_diagnosis_code,
    me.procedure_code AS medical_event_procedure_code,
    me.medication_code AS medical_event_medication_code,
    me.notes AS medical_event_notes,
    -- Prescription Information
    pr.prescription_id AS prescription_id,
    pr.medication_code AS prescription_medication_code,
    pr.fill_date AS prescription_fill_date,
    pr.days_supply AS prescription_days_supply,
    pr.quantity AS prescription_quantity
FROM 
    healthcare_analytics.gold.dim_patient dp
LEFT JOIN healthcare_analytics.gold.dim_provider prov
    ON dp.dim_provider_key = prov.id
LEFT JOIN healthcare_analytics.gold.dim_facility fac
    ON prov.dim_facility_key = fac.id
LEFT JOIN healthcare_analytics.gold.care_gaps_fact cg
    ON dp.id = cg.dim_patient_key
LEFT JOIN healthcare_analytics.gold.claims_fact cl
    ON dp.id = cl.dim_patient_key
LEFT JOIN healthcare_analytics.gold.medical_events_fact me
    ON dp.id = me.dim_patient_key
LEFT JOIN healthcare_analytics.gold.prescription_fact pr
    ON dp.id = pr.dim_patient_key
ORDER BY dp.patient_id;