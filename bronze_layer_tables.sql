CREATE OR REPLACE TABLE HEALTHCARE_ANALYTICS.BRONZE.PATIENT (
patient_id STRING,
first_name STRING,
last_name STRING,date_of_birth DATE,
gender STRING,
race STRING,
ethnicity STRING,
address STRING,
city STRING,
state STRING,
zip_code STRING,
insurance_type STRING,
primary_care_provider_id STRING,
synced_datetime timestamp_ntz);


CREATE OR REPLACE TABLE HEALTHCARE_ANALYTICS.BRONZE.PROVIDER (
provider_id STRING,
provider_name STRING,
specialty STRING,
npi_number STRING,
facility_id STRING,
synced_datetime timestamp_ntz);

CREATE OR REPLACE TABLE HEALTHCARE_ANALYTICS.BRONZE.MEDICAL_EVENTS (
event_id STRING,
patient_id STRING,
event_date TIMESTAMP_NTZ,
event_type STRING,
diagnosis_code STRING,
procedure_code STRING,
medication_code STRING,
provider_id STRING,
facility_id STRING,
notes STRING,
synced_datetime timestamp_ntz);

CREATE OR REPLACE TABLE HEALTHCARE_ANALYTICS.BRONZE.CARE_GAPS (
gap_id STRING,
patient_id STRING,
gap_type STRING,
identified_date DATE,
status STRING,
recommended_action STRING,
synced_datetime timestamp_ntz);


CREATE OR REPLACE TABLE HEALTHCARE_ANALYTICS.BRONZE.CLAIMS (
claim_id STRING,
patient_id STRING,
service_date DATE,
claim_date DATE,
claim_amount FLOAT,
claim_status STRING,
provider_id STRING,
diagnosis_codes ARRAY,
procedure_codes ARRAY,
synced_datetime timestamp_ntz);

CREATE OR REPLACE TABLE HEALTHCARE_ANALYTICS.BRONZE.PHARMACY (
prescription_id STRING,
patient_id STRING,
medication_code STRING,
fill_date DATE,
days_supply INT,
quantity FLOAT,
pharmacy_id STRING,
synced_datetime timestamp_ntz);

CREATE OR REPLACE TABLE HEALTHCARE_ANALYTICS.BRONZE.FACILITY (
facility_id STRING,
facility_name STRING,
facility_address STRING,
facility_state STRING,
facility_country STRING,
facility_zipcode STRING,
synced_datetime timestamp_ntz);