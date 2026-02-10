{% test exposure_not_too_early(model, assigned_ts, exposure_ts, grace_minutes) %}

select *
from {{ model }}
where {{ exposure_ts }} is not null
  and {{ exposure_ts }} < dateadd('minute', -1 * {{ grace_minutes }}, {{ assigned_ts }})

{% endtest %}
