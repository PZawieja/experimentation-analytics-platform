{{ config(materialized='table') }}

-- Grain: experiment_id + unit_id (one row per assignment)
-- Join canonical assignments to exposures and compute validation flags.
-- TODO: adjust column names to match your assignment model.

with assignments as (
    select
        experiment_id,
        unit_id,
        assigned_variation_id,
        assigned_at
    from {{ ref('fct_experiment_assignments') }}
)

, exposures as (
    select
        experiment_id,
        unit_id,
        variation_id,
        occurred_at,
        event_id
    from {{ ref('int_experiment_exposures_deduped') }}
)

, exposure_stats as (
    select
        experiment_id,
        unit_id,
        count(*) as exposure_event_count,
        count(distinct variation_id) as exposure_variation_count
    from exposures
    group by 1,2
)

, ranked_exposures_any as (
    select
        a.experiment_id,
        a.unit_id,
        a.assigned_variation_id,
        a.assigned_at,
        e.occurred_at as exposure_at,
        e.variation_id as exposure_variation_id,
        e.event_id as exposure_event_id,
        row_number() over (
            partition by a.experiment_id, a.unit_id
            order by e.occurred_at asc, e.event_id asc
        ) as rn_first_exposure_any
    from assignments a
    left join exposures e
        on a.experiment_id = e.experiment_id
       and a.unit_id = e.unit_id
)

, first_exposure_any as (
    select
        experiment_id,
        unit_id,
        assigned_variation_id,
        assigned_at,
        exposure_at as first_exposure_any_at,
        exposure_variation_id as first_exposure_any_variation_id,
        exposure_event_id as first_exposure_any_event_id
    from ranked_exposures_any
    where rn_first_exposure_any = 1
)

, ranked_exposures_post as (
    select
        a.experiment_id,
        a.unit_id,
        a.assigned_variation_id,
        a.assigned_at,
        e.occurred_at as exposure_at,
        e.variation_id as exposure_variation_id,
        e.event_id as exposure_event_id,
        row_number() over (
            partition by a.experiment_id, a.unit_id
            order by e.occurred_at asc, e.event_id asc
        ) as rn_first_exposure_post
    from assignments a
    left join exposures e
        on a.experiment_id = e.experiment_id
       and a.unit_id = e.unit_id
       and e.occurred_at >= dateadd(
            'minute',
            -1 * {{ var('exp_allow_pre_assignment_exposure_grace_minutes', 5) }},
            a.assigned_at
        )
)

, first_exposure_post as (
    select
        experiment_id,
        unit_id,
        assigned_variation_id,
        assigned_at,
        exposure_at as first_exposure_at,
        exposure_variation_id as first_exposure_variation_id,
        exposure_event_id as first_exposure_event_id
    from ranked_exposures_post
    where rn_first_exposure_post = 1
)

select
    a.experiment_id,
    a.unit_id,
    a.assigned_variation_id,
    a.assigned_at,

    fe_any.first_exposure_any_at,
    fe_any.first_exposure_any_variation_id,
    fe_any.first_exposure_any_event_id,

    fe_post.first_exposure_at,
    fe_post.first_exposure_variation_id,
    fe_post.first_exposure_event_id,

    datediff('second', a.assigned_at, fe_post.first_exposure_at) as exposure_delay_seconds,

    case when fe_any.first_exposure_any_at is not null then true else false end as has_any_exposure,
    case when fe_post.first_exposure_at is not null then true else false end as has_post_assignment_exposure,

    case
        when fe_post.first_exposure_at is null then false
        when fe_post.first_exposure_at > dateadd('day', {{ var('exp_exposure_max_days_after_assignment', 7) }}, a.assigned_at) then false
        when fe_post.first_exposure_variation_id != a.assigned_variation_id then false
        when es.exposure_variation_count > 1 then false
        else true
      end as has_valid_exposure

    , case
        when fe_any.first_exposure_any_at is null then false
        when fe_any.first_exposure_any_at < dateadd('minute', -1 * {{ var('exp_allow_pre_assignment_exposure_grace_minutes', 5) }}, a.assigned_at)
          then true
        else false
      end as is_pre_assignment_exposure

    , case
        when fe_post.first_exposure_at is null then false
        when fe_post.first_exposure_variation_id != a.assigned_variation_id then true
        else false
      end as is_variant_mismatch

    , case
        when es.exposure_variation_count > 1 then true
        else false
      end as has_multiple_variations_exposed

    , case
        when fe_post.first_exposure_at is null then false
        when fe_post.first_exposure_at > dateadd('day', {{ var('exp_exposure_max_days_after_assignment', 7) }}, a.assigned_at) then true
        else false
      end as exposure_outside_window

    , {{ experiment_validation_status(
        'case when fe_any.first_exposure_any_at is not null then true else false end',
        'has_multiple_variations_exposed',
        'is_pre_assignment_exposure',
        'exposure_outside_window',
        'is_variant_mismatch',
        'has_valid_exposure'
      ) }} as validation_status

from assignments a
left join first_exposure_any fe_any
    on a.experiment_id = fe_any.experiment_id
   and a.unit_id = fe_any.unit_id
left join first_exposure_post fe_post
    on a.experiment_id = fe_post.experiment_id
   and a.unit_id = fe_post.unit_id
left join exposure_stats es
    on a.experiment_id = es.experiment_id
   and a.unit_id = es.unit_id
