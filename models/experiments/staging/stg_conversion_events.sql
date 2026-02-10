{{ config(materialized='view') }}

with base as (
    select
        event_id
        -- precedence: occurred_at -> event_timestamp -> timestamp
        , coalesce(occurred_at, event_timestamp, timestamp) as occurred_at
        -- precedence: unit_id -> user_id -> account_id
        , coalesce(unit_id, user_id, account_id) as unit_id
        , event_name
    from {{ ref('fct_events') }}
    where event_name = '{{ var("exp_conversion_event_name", "purchase") }}'
)

select
    event_id
    , occurred_at
    , unit_id
    , event_name
from base
where unit_id is not null
  and occurred_at is not null
