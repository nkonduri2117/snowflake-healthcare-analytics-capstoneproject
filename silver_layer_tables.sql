create or replace transient table HEALTHCARE_ANALYTICS.silver.stg_facility (
facility_id STRING,
facility_name STRING,
facility_address STRING,
facility_state STRING,
facility_country STRING,
facility_zipcode STRING,
synced_datetime timestamp_ntz
);

create or replace transient table HEALTHCARE_ANALYTICS.silver.stg_provider (
provider_id STRING,
provider_name STRING,
speciality STRING,
npi_number STRING,
dim_facility_key number,
synced_datetime timestamp_ntz
);

create or replace transient table HEALTHCARE_ANALYTICS.silver.stg_patient (
    PATIENT_ID VARCHAR(16777216),
	FIRST_NAME VARCHAR(16777216),
	LAST_NAME VARCHAR(16777216),
	DATE_OF_BIRTH DATE,
	GENDER VARCHAR(16777216),
	RACE VARCHAR(16777216),
	ETHNICITY VARCHAR(16777216),
	ADDRESS VARCHAR(16777216),
	CITY VARCHAR(16777216),
	STATE VARCHAR(16777216),
	ZIP_CODE VARCHAR(16777216),
	INSURANCE_TYPE VARCHAR(16777216),
	DIM_PROVIDER_KEY NUMBER,
    synced_datetime timestamp_ntz
);


CREATE OR REPLACE TRANSIENT TABLE HEALTHCARE_ANALYTICS.silver.stg_care_gaps (
    gap_id STRING,  
    gap_type STRING, 
    status STRING,                     
    recommended_action STRING, 
    identified_date timestamp_ntz,
    dim_patient_key number,
    synced_datetime timestamp_ntz
);


create or replace TRANSIENT TABLE HEALTHCARE_ANALYTICS.SILVER.STG_CLAIMS (
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


CREATE OR REPLACE TRANSIENT TABLE HEALTHCARE_ANALYTICS.SILVER.STG_MEDICAL_EVENTS (
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

CREATE OR REPLACE TRANSIENT TABLE HEALTHCARE_ANALYTICS.SILVER.STG_PRESCRIPTION (
prescription_id STRING,
medication_code STRING,
fill_date DATE,
days_supply INT,
quantity FLOAT,
dim_patient_key number,
dim_facility_key number,
synced_datetime timestamp_ntz);


