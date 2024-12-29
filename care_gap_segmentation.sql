CREATE OR REPLACE PROCEDURE gold.care_gap_analysis()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_patient_id STRING;
    v_care_gap_category STRING;
    v_care_gap_score INT;
    v_event_count INT;
    v_id INT;
        c1 CURSOR for 
        SELECT id, patient_id FROM gold.dim_patient;
BEGIN
        FOR patient IN c1
        DO
            v_care_gap_category := 'No Care Gap';
            v_care_gap_score := 0;
            v_id := patient.id;
            v_patient_id := patient.patient_id; 
 
            SELECT COUNT(1) INTO v_event_count
             FROM gold.care_gaps_fact
             WHERE dim_patient_key = :v_id
               AND gap_type = 'Preventive Care'
               AND identified_date > CURRENT_DATE - INTERVAL '1 YEAR';
            IF (v_event_count = 0) THEN
              v_care_gap_category := 'Preventive Care Gap';
              v_care_gap_score := v_care_gap_score + 1;
            END IF;

            SELECT COUNT(1) INTO v_event_count
             FROM gold.care_gaps_fact
             WHERE dim_patient_key = :v_id
               AND gap_type = 'Chronic Care'
               AND identified_date > CURRENT_DATE - INTERVAL '1 YEAR';
            IF (v_event_count = 0) THEN
              v_care_gap_category := 'Chronic Care Gap';
              v_care_gap_score := v_care_gap_score + 1;
            END IF;

            SELECT COUNT(1) INTO v_event_count
             FROM gold.care_gaps_fact
             WHERE dim_patient_key = :v_id
               AND gap_type = 'Missed Appointment'
               AND identified_date > CURRENT_DATE - INTERVAL '1 YEAR';
               
            IF (v_event_count = 0) THEN
              v_care_gap_category := 'Missed Appointment Gap';
              v_care_gap_score := v_care_gap_score + 1;
            END IF;

            IF (v_care_gap_score > 0) THEN
                INSERT INTO gold.care_gaps 
                (
                patient_id, 
                gap_type, 
                identified_date, 
                status, 
                recommended_action
                )
                VALUES 
                (
                :v_patient_id, 
                :v_care_gap_category, 
                SYSDATE(), 
                'Identified', 
                'Recommend necessary care'
                );
        END IF;
            
    END FOR;

    RETURN 'Care gap analysis completed successfully';
END;
$$;