"""
The sipmliest way to calculate avarage WAU during the month. Tiaking into account only full weeks (7 days)
jinja masks are used for dinamic filtering of graphs
"""
select 
  toStartOfMonth(day_date) as to_month_date, 
  intDiv(toDayOfMonth(day_date), 7) as week_number, 
  count(distinct user_id) as uniq_users
from scheme_name.events e2 
where TRUE
  {% if from_dttm is not none %}
  AND day_date >= toDate('{{ from_dttm }}')
  {% endif %}
  {% if to_dttm is not none %}
  AND day_date < toDate('{{ to_dttm }}')
  {% endif %}
  {% if filter_values('event_type') %}
  AND events.event_type IN {{ filter_values('event_type')|where_in }}
  {% endif %}
  {% if filter_values('event_name') %}
  AND events.event_name IN {{ filter_values('event_name')|where_in }}
  {% endif %}
GROUP by toStartOfMonth(day_date), intDiv(toDayOfMonth(day_date), 7)