{{ config(materialized='view') }}

-- BI-ready audit lens for exposure quality per experiment/day.

with v as (
    select
        experiment_id,
        date_trunc('day', assigned_at) as date_day,
        has_post_assignment_exposure,
        has_valid_exposure,
        is_variant_mismatch,
        has_multiple_variations_exposed,
        is_pre_assignment_exposure,
        exposure_outside_window,
        exposure_delay_seconds
    from {{ ref('int_experiment_exposure_validation') }}
)

select
    experiment_id,
    date_day,

    count(*) as assigned_units,
    sum(case when has_post_assignment_exposure = true then 1 else 0 end) as exposed_units,
    sum(case when has_valid_exposure = true then 1 else 0 end) as valid_exposed_units,

    (
        sum(case when has_post_assignment_exposure = true then 1 else 0 end)
        / nullif(count(*), 0)
    )::float as exposure_rate,
    (
        sum(case when has_valid_exposure = true then 1 else 0 end)
        / nullif(count(*), 0)
    )::float as valid_exposure_rate,

    sum(case when is_variant_mismatch = true then 1 else 0 end) as mismatch_units,
    sum(case when has_multiple_variations_exposed = true then 1 else 0 end) as multi_variation_units,
    sum(case when is_pre_assignment_exposure = true then 1 else 0 end) as pre_assignment_units,
    sum(case when exposure_outside_window = true then 1 else 0 end) as outside_window_units,

    avg(case when has_post_assignment_exposure = true then exposure_delay_seconds else null end)
        as avg_exposure_delay_seconds,

    case
        when (
            sum(case when has_post_assignment_exposure = true then 1 else 0 end)
            / nullif(count(*), 0)
        )::float < 0.7 then 'critical'
        when (
            sum(case when has_post_assignment_exposure = true then 1 else 0 end)
            / nullif(count(*), 0)
        )::float < 0.9 then 'warning'
        else 'ok'
    end as exposure_health

from v
group by 1,2
