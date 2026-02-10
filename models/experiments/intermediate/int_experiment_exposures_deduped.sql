{{ config(materialized='table') }}

-- Deterministic exposure dedupe: keep earliest event per (experiment_id, unit_id, variation_id).
-- TODO: Adjust the dedupe window or key if your exposure events are noisy.

with src as (
    select *
    from {{ ref('stg_experiment_exposures') }}
)

, ranked as (
    select
        experiment_id,
        unit_id,
        variation_id,
        occurred_at,
        event_id,
        ingested_at,
        row_number() over (
            partition by experiment_id, unit_id, variation_id
            order by
                occurred_at asc,
                case when ingested_at is null then 1 else 0 end asc,
                ingested_at asc,
                event_id asc
        ) as rn
    from src
)

select
    experiment_id,
    unit_id,
    variation_id,
    occurred_at,
    event_id,
    ingested_at
from ranked
where rn = 1
