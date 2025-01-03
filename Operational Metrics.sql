select
    sum(credits_used)
from
    snowflake.account_usage.metering_history
where
    start_time = :daterange;


select
    count(*) as number_of_jobs
from
    snowflake.account_usage.query_history
where
    start_time >= date_trunc(month, current_date);

select
    avg(storage_bytes + stage_bytes + failsafe_bytes) / power(1024, 2) as billable_mb
from
    snowflake.account_usage.storage_usage
where
    USAGE_DATE = current_date() -1;

select
    warehouse_name,
    sum(credits_used) as total_credits_used
from
    snowflake.account_usage.warehouse_metering_history
where
    start_time = :daterange
group by
    1
order by
    2 desc;

select
    start_time::date as usage_date,
    warehouse_name,
    sum(credits_used) as total_credits_used
from
    snowflake.account_usage.warehouse_metering_history
where
    start_time = :daterange
group by
    1,
    2
order by
    2,
    1;

select
    date_trunc('HOUR', usage_date) as Usage_Month,
    sum(CREDITS_BILLED)
from
    snowflake.account_usage.metering_daily_history
group by
    Usage_Month;

select
    query_type,
    warehouse_size,
    avg(execution_time) / 1000 as average_execution_time
from
    snowflake.account_usage.query_history
where
    start_time = :daterange
group by
    1,
    2
order by
    3 desc;