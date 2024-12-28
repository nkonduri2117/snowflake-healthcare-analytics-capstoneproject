CREATE OR ALTER TABLE HEALTHCARE_ANALYTICS.gold.dim_patient (
    id number,
    patient_id STRING,     -- Unique patient identifier
    first_name STRING,                 -- First name of the patient
    last_name STRING,                  -- Last name of the patient
    date_of_birth DATE,                -- Date of birth of the patient
    gender STRING,                     -- Gender of the patient
    race STRING,                       -- Race of the patient
    ethnicity STRING,                  -- Ethnicity of the patient
    address STRING,                    -- Address of the patient
    city STRING,                       -- City of the patient
    state STRING,                      -- State of the patient
    zip_code STRING,                   -- Zip code of the patient
    insurance_type STRING,             -- Type of insurance (e.g., "Private", "Medicare", "Medicaid")
    dim_provider_key NUMBER,   -- Primary care provider associated with the patient
    who_created STRING,
    when_created timestamp_ntz,                    -- Date when the patient joined the system
    who_updated STRING,
    when_updated timestamp_ntz,
    synced_datetime timestamp_ntz
);

CREATE OR ALTER TABLE HEALTHCARE_ANALYTICS.gold.dim_provider (
    id number,
    provider_id STRING,            -- Surrogate key
    provider_name STRING,          -- Name of the provider
    specialty STRING,              -- Medical specialty of the provider
    npi_number STRING,            -- National Provider Identifier (NPI)
    dim_facility_key number,           -- Facility where the provider works
    record_status STRING,         -- 'Active' or 'Inactive'
    who_created STRING,
    when_created timestamp_ntz,
    who_updated STRING,-- Date when the patient joined the system
    when_updated timestamp_ntz,
    synced_datetime timestamp_ntz
);

CREATE OR ALTER TABLE HEALTHCARE_ANALYTICS.gold.dim_facility(
id number,
facility_id string,
facility_name string,
facility_address string,
facility_state string,
facility_country string,
facility_zipcode string,
who_created string,
when_created timestamp_ntz,
who_updated string,
when_updated timestamp_ntz,
synced_datetime timestamp_ntz
);


CREATE OR REPLACE TABLE HEALTHCARE_ANALYTICS.gold.care_gaps_fact (
    gap_key number,
    gap_id STRING,         
    gap_type STRING, 
    identified_date timestamp_ntz,                                
    status STRING,                     
    recommended_action STRING,
    dim_patient_key number,
    synced_datetime timestamp_ntz
);


create or replace TABLE HEALTHCARE_ANALYTICS.GOLD.CLAIMS_FACT (
	CLAIM_KEY NUMBER,
    CLAIM_ID VARCHAR(16777216),
	SERVICE_DATE DATE,
	CLAIM_DATE DATE,
	CLAIM_AMOUNT FLOAT,
	CLAIM_STATUS VARCHAR(16777216),
	DIAGNOSIS_CODES ARRAY,
	PROCEDURE_CODES ARRAY,
    DIM_PROVIDER_KEY NUMBER,
    DIM_PATIENT_KEY NUMBER,
    SYNCED_DATETIME TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE HEALTHCARE_ANALYTICS.GOLD.MEDICAL_EVENTS_FACT (
id number,
event_id STRING,
event_date TIMESTAMP_NTZ,
event_type STRING,
diagnosis_code STRING,
procedure_code STRING,
medication_code STRING,
notes STRING,
dim_patient_key number,
DIM_PROVIDER_KEY number,
dim_facility_key number,
synced_datetime timestamp_ntz);


CREATE OR REPLACE TABLE HEALTHCARE_ANALYTICS.GOLD.PRESCRIPTION_FACT (
id number,
prescription_id STRING,
medication_code STRING,
fill_date DATE,
days_supply INT,
quantity FLOAT,
dim_patient_key number,
dim_facility_key number,
synced_datetime timestamp_ntz);