{{ config(materialized='table') }}

with cohort as (
    select
        experiment_id
        , unit_id
        , assigned_variation_id
        , first_exposure_at
        , has_post_assignment_exposure
    from {{ ref('fct_experiment_cohort') }}
    where has_post_assignment_exposure = true
)

, conversions as (
    select
        unit_id
        , occurred_at
    from {{ ref('stg_conversion_events') }}
)

, joined as (
    select
        c.experiment_id
        , c.unit_id
        , c.assigned_variation_id as variation_id
        , c.first_exposure_at as exposure_at
        , conv.occurred_at as conversion_at
    from cohort c
    left join conversions conv
        on c.unit_id = conv.unit_id
       and conv.occurred_at >= c.first_exposure_at
       and conv.occurred_at < dateadd(
            'day',
            {{ var('exp_conversion_window_days', 7) }},
            c.first_exposure_at
        )
)

, rolled as (
    select
        experiment_id
        , unit_id
        , variation_id
        , exposure_at
        , min(conversion_at) as converted_at
    from joined
    group by 1,2,3,4
)

select
    experiment_id
    , unit_id
    , variation_id
    , exposure_at
    , '{{ var("exp_primary_metric_id", "conversion_7d") }}' as metric_id
    , {{ var('exp_conversion_window_days', 7) }} as window_days
    , case when converted_at is not null then true else false end as did_convert
    , converted_at
from rolled
