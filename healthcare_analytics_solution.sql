--create database for healthcare analytics

CREATE DATABASE HEALTHCARE_ANALYTICS;

USE DATABASE HEALTHCARE_ANALYTICS;

--set up the medallion schemas

CREATE SCHEMA BRONZE;

CREATE SCHEMA SILVER;

CREATE SCHEMA GOLD;

USE SCHEMA BRONZE;

-- storage integration to process data from s3

CREATE OR REPLACE STORAGE INTEGRATION s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::905418295534:role/snowflake_role'
  STORAGE_ALLOWED_LOCATIONS = ('*');

-- file format specification for raw data

CREATE OR REPLACE FILE FORMAT MY_PATIENT_FILE TYPE = CSV 
FIELD_OPTIONALLY_ENCLOSED_BY='"'
SKIP_HEADER = 1;

-- external stage for raw data in s3

CREATE OR REPLACE STAGE my_patient_stage
  STORAGE_INTEGRATION = s3_int
  URL = 's3://healthrecords29112024/patientrecords/patient_data'
  FILE_FORMAT = MY_PATIENT_FILE;

CREATE OR REPLACE STAGE my_provider_stage
  STORAGE_INTEGRATION = s3_int
  URL = 's3://healthrecords29112024/patientrecords/provider_data'
  FILE_FORMAT = MY_PATIENT_FILE;

CREATE OR REPLACE STAGE my_medical_events_stage
  STORAGE_INTEGRATION = s3_int
  URL = 's3://healthrecords29112024/patientrecords/medical_events_data'
  FILE_FORMAT = MY_PATIENT_FILE;

CREATE OR REPLACE STAGE my_care_gaps_stage
  STORAGE_INTEGRATION = s3_int
  URL = 's3://healthrecords29112024/patientrecords/care_gaps_data'
  FILE_FORMAT = MY_PATIENT_FILE;

CREATE OR REPLACE STAGE my_claims_stage
  STORAGE_INTEGRATION = s3_int
  URL = 's3://healthrecords29112024/patientrecords/claims_data'
  FILE_FORMAT = MY_PATIENT_FILE;


CREATE OR REPLACE STAGE my_pharmacy_stage
  STORAGE_INTEGRATION = s3_int
  URL = 's3://healthrecords29112024/patientrecords/pharma_data'
  FILE_FORMAT = MY_PATIENT_FILE;

CREATE OR REPLACE STAGE my_facility_stage
  STORAGE_INTEGRATION = s3_int
  URL = 's3://healthrecords29112024/patientrecords/facility_data'
  FILE_FORMAT = MY_PATIENT_FILE;

-- ddl for raw tables ( the raw tables will hold all the data received from s3 all time) 
-- the raw data in the bronze layer will be further used to process data into further layers
-- also raw data will be used for any exploratory data analysis without sourcing the data back from s3

CREATE OR REPLACE TABLE BRONZE.PATIENT (
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

CREATE OR REPLACE TABLE BRONZE.PROVIDER (
provider_id STRING,
provider_name STRING,
specialty STRING,
npi_number STRING,
facility_id STRING,
synced_datetime timestamp_ntz);

CREATE OR REPLACE TABLE BRONZE.MEDICAL_EVENTS (
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

CREATE OR REPLACE TABLE BRONZE.CARE_GAPS (
gap_id STRING,
patient_id STRING,
gap_type STRING,
identified_date DATE,
status STRING,
recommended_action STRING,
synced_datetime timestamp_ntz);

CREATE OR REPLACE TABLE BRONZE.CLAIMS (
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

CREATE OR REPLACE TABLE BRONZE.PHARMACY (
prescription_id STRING,
patient_id STRING,
medication_code STRING,
fill_date DATE,
days_supply INT,
quantity FLOAT,
pharmacy_id STRING,
synced_datetime timestamp_ntz);

CREATE OR REPLACE TABLE BRONZE.FACILITY (
facility_id STRING,
facility_name STRING,
facility_address STRING,
facility_state STRING,
facility_country STRING,
facility_zipcode STRING,
synced_datetime timestamp_ntz);

-- streams on raw tables

CREATE OR REPLACE STREAM FACILITY_STREAM ON TABLE FACILITY;
CREATE OR REPLACE STREAM PATIENT_STREAM ON TABLE PATIENT;
CREATE OR REPLACE STREAM PROVIDER_STREAM ON TABLE PROVIDER;
CREATE OR REPLACE STREAM MEDICAL_EVENTS_STREAM ON TABLE MEDICAL_EVENTS;
CREATE OR REPLACE STREAM CARE_GAPS_STREAM ON TABLE CARE_GAPS;
CREATE OR REPLACE STREAM CLAIMS_STREAM ON TABLE CLAIMS;
CREATE OR REPLACE STREAM PHARMACY_STREAM ON TABLE PHARMACY;

-- orchestration via snow pipe to ingest data from s3 into bronze layer

CREATE OR REPLACE PIPE P_PATIENT
AUTO_INGEST = TRUE 
AS
COPY INTO PATIENT FROM @my_patient_stage file_format = MY_PATIENT_FILE;

CREATE OR REPLACE PIPE P_PROVIDER 
AUTO_INGEST = TRUE 
AS
COPY INTO PROVIDER FROM @my_provider_stage file_format = MY_PATIENT_FILE;

CREATE OR REPLACE PIPE P_MEDICAL_EVENTS
AUTO_INGEST = TRUE 
AS
COPY INTO MEDICAL_EVENTS FROM @my_medical_events_stage file_format = MY_PATIENT_FILE;

CREATE OR REPLACE PIPE P_CARE_GAPS
AUTO_INGEST = TRUE 
AS
COPY INTO CARE_GAPS FROM @my_care_gaps_stage file_format = MY_PATIENT_FILE;

CREATE OR REPLACE PIPE P_CLAIMS
AUTO_INGEST = TRUE 
AS
COPY INTO CLAIMS FROM @my_claims_stage file_format = MY_PATIENT_FILE;

CREATE OR REPLACE PIPE P_PHARMA
AUTO_INGEST = TRUE 
AS
COPY INTO PHARMACY FROM @my_pharmacy_stage file_format = MY_PATIENT_FILE;


CREATE OR REPLACE PIPE P_FACILITY
AUTO_INGEST = TRUE 
AS
COPY INTO FACILITY FROM @my_facility_stage file_format = MY_PATIENT_FILE;

-- determines the pipe status

select system$pipe_status('P_FACILITY');
select system$pipe_status('P_PATIENT');
select system$pipe_status('P_PROVIDER');
select system$pipe_status('P_MEDICAL_EVENTS');
select system$pipe_status('P_CARE_GAPS');
select system$pipe_status('P_CLAIMS');
select system$pipe_status('P_PHARMA');

--check whether the data has been processed from S3 to Bronze Layer

SELECT * FROM BRONZE.FACILITY;
SELECT * FROM BRONZE.PATIENT;
SELECT * FROM BRONZE.PROVIDER;
SELECT * FROM BRONZE.MEDICAL_EVENTS;
SELECT * FROM BRONZE.CARE_GAPS;
SELECT * FROM BRONZE.CLAIMS;
SELECT * FROM BRONZE.PHARMACY;

-- Silver layer - Transient tables to hold the data to be processed incrementally

create or replace transient table silver.stg_facility (
facility_id STRING,
facility_name STRING,
facility_address STRING,
facility_state STRING,
facility_country STRING,
facility_zipcode STRING,
synced_datetime timestamp_ntz
);

create or replace transient table silver.stg_provider (
provider_id STRING,
provider_name STRING,
speciality STRING,
npi_number STRING,
dim_facility_key number,
synced_datetime timestamp_ntz
);

create or replace transient table silver.stg_patient (
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

CREATE OR REPLACE TRANSIENT TABLE silver.stg_care_gaps (
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

CREATE OR REPLACE TRANSIENT TABLE SILVER.STG_MEDICAL_EVENTS (
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

CREATE OR REPLACE TRANSIENT TABLE SILVER.STG_PRESCRIPTION (
prescription_id STRING,
medication_code STRING,
fill_date DATE,
days_supply INT,
quantity FLOAT,
dim_patient_key number,
dim_facility_key number,
synced_datetime timestamp_ntz);

-- Tables in gold layer which will be used for analytics

CREATE OR ALTER TABLE gold.dim_patient (
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

CREATE OR ALTER TABLE gold.dim_provider (
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

CREATE OR ALTER TABLE gold.dim_facility(
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

CREATE OR REPLACE TABLE gold.care_gaps_fact (
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


CREATE OR REPLACE TABLE GOLD.MEDICAL_EVENTS_FACT (
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

CREATE OR REPLACE TABLE GOLD.PRESCRIPTION_FACT (
id number,
prescription_id STRING,
medication_code STRING,
fill_date DATE,
days_supply INT,
quantity FLOAT,
dim_patient_key number,
dim_facility_key number,
synced_datetime timestamp_ntz);

-- Transform the data into Silver layer and Load the data into Gold Layer

create or replace task process_facility_data
warehouse = COMPUTE_WH 
schedule = 'USING CRON */5 * * * * UTC'
as
begin 
truncate table silver.stg_facility;
insert into silver.stg_facility
select 
facility_id, 
trim(facility_name),
coalesce(facility_address,'UNK'),
trim(facility_state),
trim(facility_country),
trim(facility_zipcode),
synced_datetime
from bronze.facility 
where 
facility_id is not null and 
synced_datetime > (select coalesce(max(synced_datetime),'1900-01-01 00:00:00.000000') from gold.dim_facility);
end;

create or replace task process_dim_facility
warehouse = COMPUTE_WH 
after process_facility_data 
as 
merge into gold.dim_facility as df using (
select 
facility_id, 
facility_name,
facility_address,
facility_state,
facility_country,
facility_zipcode,
synced_datetime
from 
silver.stg_facility
) as source 
on df.facility_id = source.facility_id 
when matched then update set 
facility_name = source.facility_name,
facility_address = source.facility_address,
facility_state = source.facility_state,
facility_country = source.facility_country,
facility_zipcode = source.facility_zipcode,
who_created = current_user(),
when_created = sysdate(),
synced_datetime =  source.synced_datetime
when not matched then 
insert (
id,
facility_id,
facility_name,
facility_address,
facility_state,
facility_country,
facility_zipcode,
who_updated,
when_updated,
synced_datetime) 
values (
gold.seq_dim_facility.nextval,
source.facility_id,
source.facility_name,
source.facility_address,
source.facility_state,
source.facility_country,
source.facility_zipcode,
current_user(),
sysdate(),
source.synced_datetime);

create or replace task process_provider_data
warehouse = COMPUTE_WH 
after process_dim_facility 
as
begin 
truncate table silver.stg_provider;
insert into silver.stg_provider
select 
provider_id, 
trim(provider_name),
coalesce(specialty,'UNK'),
trim(npi_number),
dim_facility.id as dim_facility_key,
provider.synced_datetime
from bronze.provider left outer join gold.dim_facility  
on (provider.facility_id = dim_facility.facility_id)
where 
provider.facility_id is not null and 
provider.synced_datetime > (select coalesce(max(synced_datetime),'1900-01-01 00:00:00.000000') from  gold.dim_facility);
end;

create or replace task process_dim_provider
warehouse = COMPUTE_WH 
after process_provider_data 
as 
merge into gold.dim_provider as dp using (
select 
provider_id, 
provider_name,
speciality,
npi_number,
dim_facility_key,
synced_datetime
from 
silver.stg_provider
) as source 
on dp.provider_id = source.provider_id 
when matched then update set 
provider_name = source.provider_name,
specialty = source.speciality,
npi_number = source.npi_number,
dim_facility_key = source.dim_facility_key,
who_updated = current_user(),
when_updated = sysdate(),
synced_datetime = source.synced_datetime
when not matched then 
insert (
id,
provider_id,
provider_name,
specialty,
npi_number,
dim_facility_key,
who_created,
when_created,
synced_datetime) 
values (
gold.seq_dim_provider.nextval,
source.provider_id,
source.provider_name,
source.speciality,
source.npi_number,
source.dim_facility_key,
current_user(),
sysdate(),
synced_datetime);

create or replace task process_patient_data
warehouse = COMPUTE_WH 
after process_dim_provider
as
begin 
truncate table silver.stg_patient;
insert into silver.stg_patient
select 
PATIENT_ID,
FIRST_NAME,
LAST_NAME,
DATE_OF_BIRTH,
GENDER,
RACE,
ETHNICITY,
ADDRESS,
CITY,
STATE,
ZIP_CODE,
INSURANCE_TYPE,
dim_provider.id as dim_provider_key,
patient.synced_datetime
from bronze.patient left outer join gold.dim_provider  
on (patient.PRIMARY_CARE_PROVIDER_ID = dim_provider.provider_id)
where 
patient.patient_id is not null and 
patient.synced_datetime > (select coalesce(max(synced_datetime),'1900-01-01 00:00:00.000000') from  gold.dim_patient);
end;

create or replace task process_dim_patient
warehouse = COMPUTE_WH 
after process_patient_data 
as 
merge into gold.dim_patient as dp using (
select 
PATIENT_ID,
FIRST_NAME,
LAST_NAME,
DATE_OF_BIRTH,
GENDER,
RACE,
ETHNICITY,
ADDRESS,
CITY,
STATE,
ZIP_CODE,
INSURANCE_TYPE,
DIM_PROVIDER_KEY,
synced_datetime
from 
silver.stg_patient
) as source 
on dp.patient_id = source.patient_id 
when matched then update set 
FIRST_NAME = source.FIRST_NAME,
LAST_NAME = source.LAST_NAME,
DATE_OF_BIRTH = source.DATE_OF_BIRTH,
GENDER = source.GENDER,
RACE = source.RACE,
ETHNICITY = source.ETHNICITY,
ADDRESS = source.ADDRESS,
CITY = source.CITY,
STATE = source.STATE,
ZIP_CODE = source.ZIP_CODE,
INSURANCE_TYPE = source.INSURANCE_TYPE,
dim_provider_key = source.dim_provider_key,
who_updated = current_user(),
when_updated = sysdate(),
synced_datetime = source.synced_datetime
when not matched then 
insert (
id,
PATIENT_ID,
FIRST_NAME,
LAST_NAME,
DATE_OF_BIRTH,
GENDER,
RACE,
ETHNICITY,
ADDRESS,
CITY,
STATE,
ZIP_CODE,
INSURANCE_TYPE,
DIM_PROVIDER_KEY,
who_created,
when_created,
synced_datetime) 
values (
gold.seq_dim_patient.nextval,
source.patient_id,
source.FIRST_NAME,
source.LAST_NAME,
source.DATE_OF_BIRTH,
source.GENDER,
source.RACE,
source.ETHNICITY,
source.ADDRESS,
source.CITY,
source.STATE,
source.ZIP_CODE,
source.INSURANCE_TYPE,
source.DIM_PROVIDER_KEY,
current_user(),
sysdate(),
source.synced_datetime);

create or replace task process_care_gaps_data
warehouse = COMPUTE_WH 
after process_dim_patient
when system$stream_has_data('CARE_GAPS_STREAM')
as
begin 
truncate table silver.stg_care_gaps;
insert into silver.stg_care_gaps
select 
gap_id,  
gap_type, 
status,                     
recommended_action, 
identified_date,
dim_patient.id as dim_patient_key,
care_gaps_stream.synced_datetime
from bronze.care_gaps_stream left outer join gold.dim_patient
on (care_gaps_stream.patient_id = dim_patient.patient_id)
where 
care_gaps_stream.patient_id is not null and 
metadata$action = 'INSERT';
end;

create or replace task process_care_gaps_fact
warehouse = COMPUTE_WH 
after process_care_gaps_data 
as 
merge into gold.care_gaps_fact as cgf using (
select 
gap_id,  
gap_type, 
status,                     
recommended_action, 
identified_date,
dim_patient_key,
synced_datetime
from 
silver.stg_care_gaps
) as source 
on cgf.gap_id = source.gap_id 
when matched then update set 
gap_id = source.gap_id,
gap_type = source.gap_type,
status = source.status,
recommended_action = source.recommended_action,
identified_date = source.identified_date,
dim_patient_key = source.dim_patient_key,
synced_datetime = source.synced_datetime
when not matched then 
insert (
gap_key,
gap_id,  
gap_type, 
status,                     
recommended_action, 
identified_date,
dim_patient_key,
synced_datetime
) 
values (
gold.seq_care_gaps_fact.nextval,
source.gap_id,
source.gap_type,
source.status,
source.recommended_action,
source.identified_date,
source.dim_patient_key,
source.synced_datetime
);

create or replace task process_claims_data
warehouse = COMPUTE_WH 
after process_dim_patient
when system$stream_has_data('CLAIMS_STREAM')
as
begin 
truncate table silver.stg_claims;
insert into silver.stg_claims
select 
CLAIM_ID,
SERVICE_DATE,
CLAIM_DATE,
CLAIM_AMOUNT,
CLAIM_STATUS,
DIAGNOSIS_CODES,
PROCEDURE_CODES,
dim_patient.id as dim_patient_key,
dim_provider.id as dim_provider_key,
claims_stream.synced_datetime
from bronze.claims_stream left outer join gold.dim_patient
on (claims_stream.patient_id = dim_patient.patient_id) 
left outer join gold.dim_provider on (claims_stream.provider_id = dim_provider.provider_id)
where 
claims_stream.claim_id is not null and 
metadata$action = 'INSERT';
end;

create or replace task process_claims_fact
warehouse = COMPUTE_WH 
after process_claims_data 
as 
merge into gold.claims_fact as cf using (
select 
CLAIM_ID,
SERVICE_DATE,
CLAIM_DATE,
CLAIM_AMOUNT,
CLAIM_STATUS,
DIAGNOSIS_CODES,
PROCEDURE_CODES,
DIM_PROVIDER_KEY,
DIM_PATIENT_KEY,
SYNCED_DATETIME
from 
silver.stg_claims
) as source 
on cf.claim_id = source.claim_id 
when matched then update set 
SERVICE_DATE = source.SERVICE_DATE,
CLAIM_DATE = source.CLAIM_DATE,
CLAIM_AMOUNT = source.CLAIM_AMOUNT,
CLAIM_STATUS = source.CLAIM_STATUS,
DIAGNOSIS_CODES = source.DIAGNOSIS_CODES,
PROCEDURE_CODES = source.PROCEDURE_CODES,
DIM_PROVIDER_KEY = source.DIM_PROVIDER_KEY,
DIM_PATIENT_KEY = source.DIM_PATIENT_KEY,
SYNCED_DATETIME = source.SYNCED_DATETIME
when not matched then 
insert (
CLAIM_KEY,
CLAIM_ID,
SERVICE_DATE,
CLAIM_DATE,
CLAIM_AMOUNT,
CLAIM_STATUS,
DIAGNOSIS_CODES,
PROCEDURE_CODES,
DIM_PROVIDER_KEY,
DIM_PATIENT_KEY,
SYNCED_DATETIME) 
values (
gold.seq_claims_fact.nextval,
source.CLAIM_ID,
source.SERVICE_DATE,
source.CLAIM_DATE,
source.CLAIM_AMOUNT,
source.CLAIM_STATUS,
source.DIAGNOSIS_CODES,
source.PROCEDURE_CODES,
source.DIM_PROVIDER_KEY,
source.DIM_PATIENT_KEY,
source.SYNCED_DATETIME
);

create or replace task process_medical_events_data
warehouse = COMPUTE_WH 
after process_dim_patient
when system$stream_has_data('MEDICAL_EVENTS_STREAM')
as
begin 
truncate table silver.stg_medical_events;
insert into silver.stg_medical_events
select 
event_id,
event_date,
event_type,
diagnosis_code,
procedure_code,
medication_code,
notes,
dim_patient.id as dim_patient_key,
dim_provider.id as dim_provider_key,
dim_facility.id as dim_facility_key,
medical_events_stream.synced_datetime
from bronze.medical_events_stream 
left outer join gold.dim_patient on (medical_events_stream.patient_id = dim_patient.patient_id)
left outer join gold.dim_provider on (medical_events_stream.provider_id = dim_provider.provider_id)
left outer join gold.dim_facility on (medical_events_stream.facility_id = dim_facility.facility_id)
where 
medical_events_stream.event_id is not null and 
metadata$action = 'INSERT';
end;

create or replace task process_medical_events_fact
warehouse = COMPUTE_WH 
after process_medical_events_data 
as 
merge into gold.medical_events_fact as mef using (
select 
event_id,
event_date,
event_type,
diagnosis_code,
procedure_code,
medication_code,
notes,
dim_patient_key,
DIM_PROVIDER_KEY,
dim_facility_key,
synced_datetime
from 
silver.stg_medical_events
) as source 
on mef.event_id = source.event_id 
when matched then update set 
event_date = source.event_date,
event_type = source.event_type,
diagnosis_code = source.diagnosis_code,
procedure_code = source.procedure_code,
medication_code = source.medication_code,
notes = source.notes,
dim_patient_key = source.dim_patient_key,
DIM_PROVIDER_KEY = source.DIM_PROVIDER_KEY,
dim_facility_key = source.dim_facility_key,
synced_datetime = source.synced_datetime
when not matched then 
insert (
id,
event_id,
event_date,
event_type,
diagnosis_code,
procedure_code,
medication_code,
notes,
dim_patient_key,
DIM_PROVIDER_KEY,
dim_facility_key,
synced_datetime
) 
values (
gold.seq_event_fact.nextval,
source.event_id,
source.event_date,
source.event_type,
source.diagnosis_code,
source.procedure_code,
source.medication_code,
source.notes,
source.dim_patient_key,
source.DIM_PROVIDER_KEY,
source.dim_facility_key,
source.synced_datetime
);

create or replace task process_prescription_data
warehouse = COMPUTE_WH 
after process_dim_patient
when system$stream_has_data('PHARMACY_STREAM')
as
begin 
truncate table silver.stg_prescription;
insert into silver.stg_prescription
select 
prescription_id,
medication_code,
fill_date,
days_supply,
quantity,
dim_patient.id as dim_patient_key,
dim_facility.id as dim_facility_key,
pharmacy_stream.synced_datetime
from bronze.pharmacy_stream 
left outer join gold.dim_patient on (pharmacy_stream.patient_id = dim_patient.patient_id)
left outer join gold.dim_facility on (pharmacy_stream.pharmacy_id = dim_facility.facility_id)
where 
pharmacy_stream.prescription_id is not null and 
metadata$action = 'INSERT';
end;

create or replace task process_prescription_fact
warehouse = COMPUTE_WH 
after process_prescription_data 
as 
merge into gold.prescription_fact as pf using (
select 
prescription_id,
medication_code,
fill_date,
days_supply,
quantity,
dim_patient_key,
dim_facility_key,
synced_datetime
from 
silver.stg_prescription
) as source 
on pf.prescription_id = source.prescription_id 
when matched then update set 
prescription_id = source.prescription_id,
medication_code = source.medication_code,
fill_date  = source.fill_date,
days_supply = source.days_supply,
quantity = source.quantity,
dim_patient_key = source.dim_patient_key,
dim_facility_key = source.dim_facility_key,
synced_datetime = source.synced_datetime
when not matched then 
insert (
id,
prescription_id,
medication_code,
fill_date,
days_supply,
quantity,
dim_patient_key,
dim_facility_key,
synced_datetime
) 
values (
gold.seq_prescription_fact.nextval,
source.prescription_id,
source.medication_code,
source.fill_date,
source.days_supply,
source.quantity,
source.dim_patient_key,
source.dim_facility_key,
source.synced_datetime
);

alter task process_prescription_fact resume;
alter task process_prescription_data resume;

alter task process_medical_events_fact resume;
alter task process_medical_events_data resume;

alter task process_claims_fact resume;
alter task process_claims_data resume;

alter task process_care_gaps_fact resume;
alter task process_care_gaps_data resume;

alter task process_dim_patient resume;
alter task process_patient_data resume;

alter task  process_dim_provider resume;
alter task process_provider_data resume;

alter task process_dim_facility resume;
alter task process_facility_data resume;
