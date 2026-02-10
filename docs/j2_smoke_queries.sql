-- J2 Smoke Queries (Snowflake)
-- Replace date filters or experiment_id as needed.

-- 1) Count assignments vs validation rows
select
    count(*) as assignment_rows
from fct_experiment_assignments;

select
    count(*) as validation_rows
from int_experiment_exposure_validation;

-- 2) Top experiments by lowest exposure_rate in last 7 days
select
    experiment_id,
    avg(exposure_rate) as avg_exposure_rate_7d
from fct_experiment_exposure_quality_daily
where date_day >= dateadd('day', -7, current_date())
group by 1
order by avg_exposure_rate_7d asc
limit 10;

-- 3) Examples of variant mismatch records
select *
from int_experiment_exposure_validation
where validation_status = 'variant_mismatch'
limit 50;

-- 4) Examples of pre-assignment exposure records
select *
from int_experiment_exposure_validation
where validation_status = 'pre_assignment_exposure'
limit 50;

-- 5) Distribution of exposure_delay_seconds for a chosen experiment_id
-- Set a query parameter named EXPERIMENT_ID if your SQL client supports it.
select
    width_bucket(exposure_delay_seconds, 0, 86400, 24) as delay_bucket,
    count(*) as n
from int_experiment_exposure_validation
where experiment_id = {{ var('experiment_id', "'exp_0'") }}
  and has_any_exposure = true
group by 1
order by 1;
