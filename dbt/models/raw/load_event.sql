-- dbt/models/raw/load_events.sql

select
  id,
  event_type,
  occurred_at
from {{ source('external', 'events') }}
