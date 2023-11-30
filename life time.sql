SELECT 
    user_id, role_type, user_type, toMonday(user_created_at) as user_created_at,
    (toUnixTimestamp(max(me.created_at)) - toUnixTimestamp(min(me.created_at))) / (3600 * 24)) AS lifetime_circle_in_days
FROM punch.mixed_events me
GROUP BY user_id, role_type, user_type, toMonday(user_created_at)