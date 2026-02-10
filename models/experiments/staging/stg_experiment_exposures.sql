{{ config(materialized='view') }}

with base as (
    select
        event_id,
        -- precedence: occurred_at -> event_timestamp -> timestamp
        coalesce(occurred_at, event_timestamp, timestamp) as occurred_at,
        -- precedence: unit_id -> user_id -> account_id -> properties:user_id
        coalesce(unit_id, user_id, account_id, try_to_varchar(properties:user_id)) as unit_id,
        -- precedence: column -> properties -> event_properties
        coalesce(
            experiment_id,
            try_to_varchar(properties:experiment_id),
            try_to_varchar(event_properties:experiment_id)
        ) as experiment_id,
        -- precedence: column -> properties -> event_properties
        coalesce(
            variation_id,
            try_to_varchar(properties:variation_id),
            try_to_varchar(event_properties:variation_id)
        ) as variation_id,
        ingested_at
    from {{ ref('fct_events') }}
    where event_name = '{{ var("exp_exposure_event_name", "experiment_exposed") }}'
)

select
    event_id,
    occurred_at,
    unit_id,
    experiment_id,
    variation_id,
    ingested_at
from base
where experiment_id is not null
  and unit_id is not null
  and occurred_at is not null
