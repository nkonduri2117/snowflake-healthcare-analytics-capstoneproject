create resource monitor HEALTHCARE_ANALYTICS_MONITOR with credit_quota=500
frequency = daily
start_timestamp = IMMEDIATELY 
notify_users = (NAKOND77, "NAKOND77")
triggers on 90 percent do notify;

ALTER WAREHOUSE COMPUTE_WH SET RESOURCE_MONITOR = HEALTHCARE_ANALYTICS_MONITOR;