{{ config(materialized='table') }}

select
    1 as sanity_id
    , 'duckdb_ok' as sanity_status
