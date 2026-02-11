{{ config(materialized='table') }}

select
    'source' as asset_type
    , '_silver.int_experiment_exposure_validation' as asset_name
    , 'experiment_id, user_id' as grain
    , 'experiment_id, user_id' as primary_keys
    , true as is_allowed_for_ai
    , 'Assignment-to-exposure validation at experiment x user grain. Use for exposure integrity and cohort validation.' as description

union all
select
    'source'
    , '_silver.int_experiment_exposures_deduped'
    , 'experiment_id, user_id, variant_id'
    , 'experiment_id, user_id, variant_id'
    , true
    , 'Deduped exposure events (earliest per experiment/user/variant). Use to compute exposure rates and timing.'

union all
select
    'source'
    , '_gold.fct_experiment_quality_metrics_daily'
    , 'experiment_id, date_day'
    , 'experiment_id, date_day'
    , true
    , 'Daily quality metrics for exposure validation. Use for monitoring (exposure_rate, mismatch_rate, etc.).'

