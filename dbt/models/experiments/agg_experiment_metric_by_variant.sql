with outcomes as (

    select
        experiment_id
        , user_id
        , assigned_variant_id
        , is_converted_7d
    from {{ ref('int_experiment_metric_outcomes__conversion') }}

)

select
    experiment_id
    , assigned_variant_id as variation_id
    , count(*) as n_users
    , sum(is_converted_7d) as n_converted
    , round(sum(is_converted_7d) * 1.0 / count(*), 4) as conversion_rate
from outcomes
group by 1,2
