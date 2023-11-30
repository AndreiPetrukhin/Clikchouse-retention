-- This script allows to calculate timespent for daily, weekly, monthly windows.
-- May be more then one event. 
-- Date filters allowed also.
-- May choose the feed_id
with cte as (SELECT
		platform_id,
        user_id,
        created_at,
        prev_created_at,
        video_id,
        SUM(IF((toUnixTimestamp(created_at) - toUnixTimestamp(prev_created_at)) > 900, 1, 0))
            OVER (PARTITION BY user_id ORDER BY created_at) AS user_session_id
FROM (SELECT DISTINCT
        user_id,
        created_at,
        any(created_at) OVER (PARTITION BY user_id, toDate(created_at) ORDER BY created_at rows BETWEEN 1 PRECEDING AND 1 PRECEDING) as prev_created_at,
        video_id,
        app_context_id
    FROM events
    WHERE True = True 
    	and toDate(events.created_at) >= toDate('2023-03-22')
    	and toDate(events.created_at) <= toDate('2023-03-27')
    	and events.event_type = 'VIDEO'
    	and events.event_name = 'VIEWING'
    	and simpleJSONExtractString(attributes_map, 'feed_id') = 'manual-top-audio-config'
    	) events_with_previous_timestamp
JOIN (
    SELECT
        IF(LENGTH(app_context_id) = 16, toString(toUUID(hex(app_context_id))), app_context_id) AS app_context_id,
        platform_id
    FROM eventaudiomanager.app_contexts
) AS app_contexts
ON (events_with_previous_timestamp.app_context_id = app_contexts.app_context_id))
	SELECT
		platform_id,
		toDate(created_at) as day_date,
		toStartOfWeek(created_at, 9) as week_date,
		toStartOfMonth(created_at) as month_date,
		user_id, 
		user_session_id,
		(toUnixTimestamp(max(created_at)) - toUnixTimestamp(min(created_at))) / 60 as session_duration_min
	FROM cte
	group by platform_id, day_date, week_date, month_date, user_id, user_session_id
	order by day_date;