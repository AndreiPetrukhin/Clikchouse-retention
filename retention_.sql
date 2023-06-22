-- This script allows to calculate retention for daily, weekly, monthly windows.
-- May be more then one event for beginning and returnging. 
-- Date filters allowed also.
with 
	('FEED') as starting_event_type,
	('OPENED') as starting_event_name,
	('FEED') as return_event_type,
	('OPENED') as return_event_name,
	toDate('2023-01-01') as starting_date,
	toDate('2023-03-31') as return_date,
	
-- Calculate the number of users who performed the starting event
starting_users AS (
SELECT 
	user_id
	, toDate(min(events.created_at)) as start_date
	, toStartOfWeek(min(events.created_at)) as start_week
	, toStartOfMonth(min(events.created_at)) as start_month
FROM events
WHERE events.event_type in starting_event_type
	and events.event_name in starting_event_name
	and toDate(events.created_at) between starting_date and return_date --including dates
group by user_id
),

-- Calculate the number of users who performed both events
cohort AS (
  SELECT 
  	toDate(events.created_at) - starting_users.start_date as ret_day
  	, (toStartOfWeek(events.created_at) - starting_users.start_week) / 7 as ret_week
  	, intDiv((toStartOfMonth(events.created_at) - starting_users.start_month), 28) as ret_month
  	, count(distinct starting_users.user_id) over(partition by ret_day) as ret_day_qty
  	, count(distinct starting_users.user_id) over(partition by ret_week) as ret_week_qty
  	, count(distinct starting_users.user_id) over(partition by ret_month) as ret_month_qty
  FROM starting_users
  JOIN events ON starting_users.user_id = events.user_id
  WHERE events.event_type in return_event_type
  	and events.event_name in return_event_name
  	and toDate(events.created_at) between starting_date and return_date --including dates
),

first_day as (
	select 
		max(ret_day_qty) as ret_day_qty
		, max(ret_week_qty) as ret_week_qty
		, max(ret_month_qty) as ret_month_qty
	from cohort
)

-- Calculate retention rate for each day after starting event
SELECT 
	cohort.ret_day
	, cohort.ret_week
	, cohort.ret_month
	, cohort.ret_day_qty
	, cohort.ret_week_qty
	, cohort.ret_month_qty
	, cohort.ret_day_qty / first_day.ret_day_qty * 100 as ret_day_percent
	, cohort.ret_week_qty / first_day.ret_week_qty * 100 as ret_week_percent
	, cohort.ret_month_qty / first_day.ret_month_qty * 100 as ret_month_percent
FROM cohort
cross join first_day
ORDER BY ret_month ASC;
