CREATE DATABASE HEALTHCARE_ANALYTICS;

CREATE SCHEMA BRONZE;

USE DATABASE HEALTHCARE_ANALYTICS;

CREATE SCHEMA SILVER;

CREATE SCHEMA GOLD;

USE SCHEMA BRONZE;

CREATE OR REPLACE STORAGE INTEGRATION s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::905418295534:role/snowflake_role'
  STORAGE_ALLOWED_LOCATIONS = ('*');


CREATE OR REPLACE FILE FORMAT MY_PATIENT_FILE TYPE = CSV 
FIELD_OPTIONALLY_ENCLOSED_BY='"'
SKIP_HEADER = 1;


CREATE OR REPLACE STAGE my_patient_stage
  STORAGE_INTEGRATION = s3_int
  URL = 's3://healthrecords29112024/patientrecords/patient_data'
  FILE_FORMAT = MY_PATIENT_FILE;


CREATE OR REPLACE TABLE PATIENT (
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
primary_care_provider_id STRING);

CREATE OR REPLACE PIPE P_PATIENT
AUTO_INGEST = TRUE 
AS
COPY INTO PATIENT FROM @my_patient_stage file_format = MY_PATIENT_FILE;

CREATE OR REPLACE TABLE PROVIDER (
provider_id STRING,
provider_name STRING,
specialty STRING,
npi_number STRING,
facility_id STRING);

CREATE OR REPLACE STAGE my_provider_stage
  STORAGE_INTEGRATION = s3_int
  URL = 's3://healthrecords29112024/patientrecords/provider_data'
  FILE_FORMAT = MY_PATIENT_FILE;

CREATE OR REPLACE PIPE P_PROVIDER 
AUTO_INGEST = TRUE 
AS
COPY INTO PROVIDER FROM @my_provider_stage file_format = MY_PATIENT_FILE;

CREATE OR REPLACE TABLE MEDICAL_EVENTS (
event_id STRING,
patient_id STRING,
event_date TIMESTAMP_NTZ,
event_type STRING,
diagnosis_code STRING,
procedure_code STRING,
medication_code STRING,
provider_id STRING,
facility_id STRING,
notes STRING);

CREATE OR REPLACE STAGE my_medical_events_stage
  STORAGE_INTEGRATION = s3_int
  URL = 's3://healthrecords29112024/patientrecords/medical_events_data'
  FILE_FORMAT = MY_PATIENT_FILE;

CREATE OR REPLACE PIPE P_MEDICAL_EVENTS
AUTO_INGEST = TRUE 
AS
COPY INTO MEDICAL_EVENTS FROM @my_medical_events_stage file_format = MY_PATIENT_FILE;

CREATE OR REPLACE TABLE CARE_GAPS (
gap_id STRING,
patient_id STRING,
gap_type STRING,
identified_date DATE,
status STRING,
recommended_action STRING);

CREATE OR REPLACE STAGE my_care_gaps_stage
  STORAGE_INTEGRATION = s3_int
  URL = 's3://healthrecords29112024/patientrecords/care_gaps_data'
  FILE_FORMAT = MY_PATIENT_FILE;

CREATE OR REPLACE PIPE P_CARE_GAPS
AUTO_INGEST = TRUE 
AS
COPY INTO CARE_GAPS FROM @my_care_gaps_stage file_format = MY_PATIENT_FILE;

CREATE OR REPLACE TABLE CLAIMS (
claim_id STRING,
patient_id STRING,
service_date DATE,
claim_date DATE,
claim_amount FLOAT,
claim_status STRING,
provider_id STRING,
diagnosis_codes ARRAY,
procedure_codes ARRAY);

CREATE OR REPLACE STAGE my_claims_stage
  STORAGE_INTEGRATION = s3_int
  URL = 's3://healthrecords29112024/patientrecords/claims_data'
  FILE_FORMAT = MY_PATIENT_FILE;

CREATE OR REPLACE PIPE P_CLAIMS
AUTO_INGEST = TRUE 
AS
COPY INTO CLAIMS FROM @my_claims_stage file_format = MY_PATIENT_FILE;

CREATE OR REPLACE TABLE PHARMACY (
prescription_id STRING,
patient_id STRING,
medication_code STRING,
fill_date DATE,
days_supply INT,
quantity FLOAT,
pharmacy_id STRING);

CREATE OR REPLACE STAGE my_pharmacy_stage
  STORAGE_INTEGRATION = s3_int
  URL = 's3://healthrecords29112024/patientrecords/pharma_data'
  FILE_FORMAT = MY_PATIENT_FILE;

CREATE OR REPLACE PIPE P_PHARMA
AUTO_INGEST = TRUE 
AS
COPY INTO PHARMACY FROM @my_pharmacy_stage file_format = MY_PATIENT_FILE;


CREATE OR REPLACE TABLE FACILITY (
facility_id STRING,
facility_name STRING,
facility_address STRING,
facility_state STRING,
facility_country STRING,
facility_zipcode STRING);

CREATE OR REPLACE STAGE my_facility_stage
  STORAGE_INTEGRATION = s3_int
  URL = 's3://healthrecords29112024/patientrecords/facility_data'
  FILE_FORMAT = MY_PATIENT_FILE;

CREATE OR REPLACE PIPE P_FACILITY
AUTO_INGEST = TRUE 
AS
COPY INTO FACILITY FROM @my_facility_stage file_format = MY_PATIENT_FILE;

select system$pipe_status('P_FACILITY');
select system$pipe_status('P_PATIENT');
select system$pipe_status('P_PROVIDER');
select system$pipe_status('P_MEDICAL_EVENTS');
select system$pipe_status('P_CARE_GAPS');
select system$pipe_status('P_CLAIMS');
select system$pipe_status('P_PHARMA');

SELECT * FROM FACILITY;
SELECT * FROM PATIENT;
SELECT * FROM PROVIDER_STREAM;
SELECT * FROM MEDICAL_EVENTS;
SELECT * FROM CARE_GAPS;
SELECT * FROM CLAIMS;
SELECT * FROM PHARMACY;

CREATE STREAM FACILITY_STREAM ON TABLE FACILITY;
CREATE STREAM PATIENT_STREAM ON TABLE PATIENT;
CREATE STREAM PROVIDER_STREAM ON TABLE PROVIDER;
CREATE STREAM MEDICAL_EVENTS_STREAM ON TABLE MEDICAL_EVENTS;
CREATE STREAM CARE_GAPS_STREAM ON TABLE CARE_GAPS;
CREATE STREAM CLAIMS_STREAM ON TABLE CLAIMS;
CREATE STREAM PHARMACY_STREAM ON TABLE PHARMACY;


CREATE TABLE gold.dim_patient (
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
    when_updated timestamp_ntz
);

CREATE TABLE gold.dim_provider (
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
    when_updated timestamp_ntz
);

CREATE TABLE gold.dim_facility(
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
when_updated timestamp_ntz
);

create or replace transient table silver.stg_facility (
facility_id STRING,
facility_name STRING,
facility_address STRING,
facility_state STRING,
facility_country STRING,
facility_zipcode STRING
);

create or replace task process_facility_data
warehouse = COMPUTE_WH 
schedule = 'USING CRON 5 * * * * UTC'
when system$stream_has_data('FACILITY_STREAM')
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
trim(facility_zipcode)
from bronze.facility_stream 
where 
facility_id is not null and 
metadata$action = 'INSERT';
end;

ALTER TASK process_facility_data RESUME;

ALTER TASK process_facility_data SUSPEND;

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
facility_zipcode from 
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
when_created = sysdate()
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
when_updated) 
values (
gold.seq_dim_facility.nextval,
source.facility_id,
source.facility_name,
source.facility_address,
source.facility_state,
source.facility_country,
source.facility_zipcode,
current_user(),
sysdate());

ALTER TASK process_dim_facility RESUME;

create or replace transient table silver.stg_provider (
provider_id STRING,
provider_name STRING,
speciality STRING,
npi_number STRING,
dim_facility_key number
);

create or replace task process_provider_data
warehouse = COMPUTE_WH 
schedule = 'USING CRON 5 * * * * UTC'
when system$stream_has_data('PROVIDER_STREAM')
as
begin 
truncate table silver.stg_provider;
insert into silver.stg_provider
select 
provider_id, 
trim(provider_name),
coalesce(specialty,'UNK'),
trim(npi_number),
dim_facility.id as dim_facility_key
from bronze.provider_stream left outer join gold.dim_facility  
on (provider_stream.facility_id = dim_facility.facility_id)
where 
provider_stream.facility_id is not null and 
metadata$action = 'INSERT';
end;

ALTER TASK process_provider_data RESUME;

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
dim_facility_key
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
when_updated = sysdate()
when not matched then 
insert (
id,
provider_id,
provider_name,
specialty,
npi_number,
dim_facility_key,
who_created,
when_created) 
values (
gold.seq_dim_provider.nextval,
source.provider_id,
source.provider_name,
source.speciality,
source.npi_number,
source.dim_facility_key,
current_user(),
sysdate());

ALTER TASK process_dim_provider RESUME;


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
	DIM_PROVIDER_KEY NUMBER
);

create or replace task process_patient_data
warehouse = COMPUTE_WH 
schedule = 'USING CRON 5 * * * * UTC'
when system$stream_has_data('PATIENT_STREAM')
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
dim_provider.id as dim_provider_key
from bronze.patient_stream left outer join gold.dim_provider  
on (patient_stream.PRIMARY_CARE_PROVIDER_ID = dim_provider.provider_id)
where 
patient_stream.patient_id is not null and 
metadata$action = 'INSERT';
end;

ALTER TASK process_patient_data RESUME;

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
DIM_PROVIDER_KEY
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
when_updated = sysdate()
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
when_created) 
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
sysdate());

ALTER TASK process_dim_patient RESUME;
ALTER TASK process_patient_data SUSPEND;
ALTER TASK process_patient_data RESUME;


CREATE OR REPLACE TABLE gold.care_gaps_fact (
    gap_key number,
    gap_id STRING,         
    gap_type STRING, 
    identified_date timestamp_ntz,                                
    status STRING,                     
    recommended_action STRING,
    dim_patient_key number
);

CREATE OR REPLACE TRANSIENT TABLE silver.stg_care_gaps (
    gap_id STRING,  
    gap_type STRING, 
    status STRING,                     
    recommended_action STRING, 
    identified_date timestamp_ntz,
    dim_patient_key number     
);

create or replace task process_care_gaps_data
warehouse = COMPUTE_WH 
schedule = 'USING CRON 5 * * * * UTC'
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
dim_patient.id as dim_patient_key
from bronze.care_gaps_stream left outer join gold.dim_patient
on (care_gaps_stream.patient_id = dim_patient.patient_id)
where 
care_gaps_stream.patient_id is not null and 
metadata$action = 'INSERT';
end;

ALTER TASK process_care_gaps_data RESUME;

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
dim_patient_key
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
dim_patient_key = source.dim_patient_key
when not matched then 
insert (
gap_key,
gap_id,  
gap_type, 
status,                     
recommended_action, 
identified_date,
dim_patient_key) 
values (
gold.seq_care_gaps_fact.nextval,
source.gap_id,
source.gap_type,
source.status,
source.recommended_action,
source.identified_date,
source.dim_patient_key
);

ALTER TASK process_care_gaps_data RESUME;
ALTER TASK process_care_gaps_fact RESUME;

create or replace TRANSIENT TABLE HEALTHCARE_ANALYTICS.SILVER.STG_CLAIMS (
	CLAIM_ID VARCHAR(16777216),
	SERVICE_DATE DATE,
	CLAIM_DATE DATE,
	CLAIM_AMOUNT FLOAT,
	CLAIM_STATUS VARCHAR(16777216),
	DIAGNOSIS_CODES ARRAY,
	PROCEDURE_CODES ARRAY,
    DIM_PROVIDER_KEY NUMBER,
    DIM_PATIENT_KEY NUMBER
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
    DIM_PATIENT_KEY NUMBER
);


create or replace task process_claims_data
warehouse = COMPUTE_WH 
schedule = 'USING CRON */5 * * * * UTC'
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
dim_provider.id as dim_provider_key
from bronze.claims_stream left outer join gold.dim_patient
on (claims_stream.patient_id = dim_patient.patient_id) 
left outer join gold.dim_provider on (claims_stream.provider_id = dim_provider.provider_id)
where 
claims_stream.claim_id is not null and 
metadata$action = 'INSERT';
end;

ALTER TASK process_claims_data RESUME;

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
DIM_PATIENT_KEY
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
DIM_PATIENT_KEY = source.DIM_PATIENT_KEY
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
DIM_PATIENT_KEY) 
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
source.DIM_PATIENT_KEY
);

ALTER TASK process_claims_data SUSPEND;
ALTER TASK process_claims_data RESUME;
ALTER TASK process_claims_fact RESUME;