{{ config(materialized='view') }}

select
    experiment_id
  , n_control
  , cr_control
  , n_treatment
  , cr_treatment
  , uplift_abs
  , z_score
  , p_value_two_sided as p_value
  , ci_low
  , ci_high
from {{ ref('fct_experiment_results') }}
