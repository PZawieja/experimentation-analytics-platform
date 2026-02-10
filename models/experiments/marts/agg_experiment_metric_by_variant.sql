{{ config(materialized='table') }}

select
    experiment_id
    , metric_id
    , variation_id

    , count(*) as n_users
    , sum(case when did_convert = true then 1 else 0 end) as conversions
    , (
        sum(case when did_convert = true then 1 else 0 end)
        / nullif(count(*), 0)
    )::float as conversion_rate
    , (
        (
            sum(case when did_convert = true then 1 else 0 end)
            / nullif(count(*), 0)
        )::float
        * (1 - (
            sum(case when did_convert = true then 1 else 0 end)
            / nullif(count(*), 0)
        )::float)
    ) / nullif(count(*), 0) as var_p

from {{ ref('int_experiment_metric_outcomes__conversion') }}
group by 1,2,3
