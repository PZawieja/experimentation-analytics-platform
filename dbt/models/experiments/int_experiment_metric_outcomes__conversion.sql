with exposure_validation as (

    select
        experiment_id
        , user_id
        , assigned_variant_id
        , assigned_at
    from {{ source('silver', 'int_experiment_exposure_validation') }}

)

, conversion_events as (

    select
        user_id
        , cast(event_ts as timestamp) as event_ts
    from {{ ref('stg_conversion_events') }}
    where event_name = '{{ var("exp_conversion_event_name") }}'

)

, joined as (

    select
        ev.experiment_id
        , ev.user_id
        , ev.assigned_variant_id
        , ev.assigned_at
        , ce.event_ts
        , case
            when ce.event_ts is not null
                 and ce.event_ts >= ev.assigned_at
                 and ce.event_ts < ev.assigned_at + interval '{{ var("exp_conversion_window_days") }} days'
                then 1
            else 0
        end as is_converted_7d
    from exposure_validation ev
    left join conversion_events ce
        on ev.user_id = ce.user_id

)

select
    experiment_id
    , user_id
    , assigned_variant_id
    , max(is_converted_7d) as is_converted_7d
from joined
group by 1,2,3
