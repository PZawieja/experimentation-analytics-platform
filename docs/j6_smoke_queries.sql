-- J6 Smoke Queries (Snowflake)
-- Replace experiment_id or date filters as needed.

-- 1) Recent results for one experiment_id
select
    experiment_id
    , metric_id
    , variation_id
    , n_users
    , conversion_rate
    , uplift_abs
    , p_value_two_sided
    , computed_at
from fct_experiment_results
where experiment_id = 'exp_0'
order by computed_at desc
limit 50;

-- 2) Compare exposure_rate with conversion results availability
with exposure as (
    select
        experiment_id
        , date_day
        , exposure_rate
    from fct_experiment_exposure_quality_daily
    where date_day >= dateadd('day', -7, current_date())
)
, results as (
    select
        experiment_id
        , max(computed_at) as last_results_at
        , count(*) as result_rows
    from fct_experiment_results
    group by 1
)
select
    e.experiment_id
    , avg(e.exposure_rate) as avg_exposure_rate_7d
    , r.last_results_at
    , r.result_rows
from exposure e
left join results r
    on e.experiment_id = r.experiment_id
group by 1,3,4
order by avg_exposure_rate_7d asc;

-- 3) Experiments with significant uplift (p < 0.05, abs(uplift_abs) > 0.01)
select
    experiment_id
    , metric_id
    , variation_id
    , uplift_abs
    , p_value_two_sided
    , ci_low
    , ci_high
from fct_experiment_results
where p_value_two_sided < 0.05
  and abs(uplift_abs) > 0.01
order by p_value_two_sided asc
limit 50;

-- 4) High mismatch_rate but results exist (integrity gating discussion)
with mismatches as (
    select
        experiment_id
        , date_day
        , variant_mismatch_rate
    from fct_experiment_exposure_quality_daily
    where date_day >= dateadd('day', -7, current_date())
)
, results as (
    select
        experiment_id
        , count(*) as result_rows
    from fct_experiment_results
    group by 1
)
select
    m.experiment_id
    , avg(m.variant_mismatch_rate) as avg_mismatch_rate_7d
    , r.result_rows
from mismatches m
join results r
    on m.experiment_id = r.experiment_id
group by 1,3
having avg(m.variant_mismatch_rate) > 0.02
order by avg_mismatch_rate_7d desc;
