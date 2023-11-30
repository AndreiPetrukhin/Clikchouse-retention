"""
The sipmliest way to calculate not new active users withtin day / week / month
jinja masks are used for dinamic filtering of graphs
"""
SELECT 
	*
FROM 
(SELECT 
	{% if filter_values('my_time_grain') %}
  to{{ filter_values('my_time_grain')[0] }}(day_date) as active_date
  {% endif %},
	user_id,
	user_type,
	role_type,
	event
FROM scheme_name.events e2 
where TRUE
  {% if from_dttm is not none %}
  AND day_date >= toDate('{{ from_dttm }}')
  {% endif %}
  {% if to_dttm is not none %}
  AND day_date < toDate('{{ to_dttm }}')
  {% endif %}
	and event in ('VIDEO_STARTED', 'VIDEO_SHOWN')
group by active_date, user_id, user_type, role_type, event
) events 
JOIN (SELECT 
		e2.user_id, min(day_date) as first_action
	FROM scheme_name.events e2 
	group by e2.user_id) first_event on first_event.user_id = events.user_id
WHERE events.active_date > first_event.first_action