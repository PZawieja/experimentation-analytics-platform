{{ config(materialized='table') }}

-- Canonical experiment cohort table (assignment + exposure validation).

with assignments as (
    select
        experiment_id,
        unit_id,
        assigned_variation_id,
        assigned_at
    from {{ ref('fct_experiment_assignments') }}
)

, validation as (
    select
        experiment_id,
        unit_id,
        first_exposure_at,
        first_exposure_variation_id,
        has_post_assignment_exposure,
        exposure_delay_seconds,
        has_valid_exposure,
        validation_status
    from {{ ref('int_experiment_exposure_validation') }}
)

select
    a.experiment_id,
    a.unit_id,
    a.assigned_variation_id,
    a.assigned_at,
    v.first_exposure_at,
    v.first_exposure_variation_id,
    v.has_post_assignment_exposure,
    v.exposure_delay_seconds,
    v.has_valid_exposure,
    v.validation_status,
    true as is_in_itt_cohort,
    v.has_post_assignment_exposure as is_in_exposure_cohort,
    v.has_valid_exposure as is_in_valid_exposure_cohort
from assignments a
left join validation v
    on a.experiment_id = v.experiment_id
   and a.unit_id = v.unit_id
