CREATE OR REPLACE PROCEDURE healthcare_data.care_gap_segmentation()
  RETURNS STRING
  LANGUAGE SQL
AS
$$
DECLARE
    v_patient_id STRING;
    v_care_gap_category STRING;
    v_care_gap_score INT;
BEGIN
    -- Loop through each patient and assess care gaps
    FOR patient IN
        SELECT DISTINCT patient_id
        FROM healthcare_data.care_gap_table
    DO
        -- Default values
        LET v_care_gap_category = 'No Care Gap';
        LET v_care_gap_score = 0;
        
        -- Check for Moderate Care Gap
        IF EXISTS (
            SELECT 1
            FROM healthcare_data.care_gap_table c
            WHERE c.patient_id = patient.patient_id
              AND c.status = 'Open'
              AND c.gap_type IN ('Preventive Care', 'Follow-up Appointment')
        ) THEN
            LET v_care_gap_category = 'Moderate Care Gap';
            LET v_care_gap_score = 1;
        END IF;
        
        -- Check for Severe Care Gap
        IF EXISTS (
            SELECT 1
            FROM healthcare_data.care_gap_table c
            WHERE c.patient_id = patient.patient_id
              AND c.status = 'Open'
              AND c.gap_type = 'Chronic Care'  -- Example: missed medication refills
        ) THEN
            LET v_care_gap_category = 'Severe Care Gap';
            LET v_care_gap_score = 2;
        END IF;
        
        -- Insert the care gap segmentation result into a table (care_gap_segments)
        INSERT INTO healthcare_data.care_gap_segments (patient_id, care_gap_category, care_gap_score)
        VALUES (patient.patient_id, v_care_gap_category, v_care_gap_score);
        
    END FOR;

    RETURN 'Care gap segmentation completed successfully';
END;
$$;
