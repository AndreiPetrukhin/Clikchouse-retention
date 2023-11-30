SELECT retention_day AS retention_day,
       COUNT(DISTINCT user_id) / MAX(users) AS "ALL",
       COUNT(DISTINCT CASE
                          WHEN platform = 'ANDROID' THEN user_id
                      END) / MAX(android_users) AS "ANDROID",
       COUNT(DISTINCT CASE
                          WHEN platform = 'IOS' THEN user_id
                      END) / MAX(ios_users) AS "IOS"
FROM
  (WITH toDate('2023-08-07T00:00:00') AS start_day,
        (toDate('2023-08-21T00:00:00') - 1) AS end_day,
        LEAST(toDate(NOW()) - 1, end_day + 180) AS return_event_end_day,
        toDateTime(return_event_end_day + interval '1 day') - interval '1 second' AS max_return_event_timestamp,
        users_with_start_events AS
     (SELECT DISTINCT platform,
                      role_type,
                      toDate(created_at) AS start_event_day,
                      toStartOfHour(created_at) AS created_at_start_of_hour,
                      user_id,
                      FLOOR((toStartOfHour(max_return_event_timestamp) - created_at_start_of_hour) / (24. * 3600.0)) AS maximum_possible_return_day
      FROM scheme_name.events
      WHERE day_date BETWEEN start_day AND end_day
        AND event_type IN ('FEED')
        AND event_name IN ('OPENED') ),
        user_maximum_possible_return_day AS
     (SELECT platform,
             user_id,
             MAX(maximum_possible_return_day) AS maximum_possible_return_day
      FROM users_with_start_events
      GROUP BY platform,
               user_id),
        user_retention_events AS
     (SELECT DISTINCT platform,
                      toStartOfHour(created_at) AS created_at_start_of_hour,
                      user_id
      FROM scheme_name.events
      WHERE day_date BETWEEN start_day AND return_event_end_day
        AND event_type IN ('FEED')
        AND event_name IN ('OPENED') ),
        base_retention AS
     (SELECT users_with_start_events.platform AS platform,
             users_with_start_events.user_id AS user_id,
             users_with_start_events.role_type AS role_type,
             users_with_start_events.start_event_day AS start_event_day,
             users_with_start_events.created_at_start_of_hour AS start_event_ts,
             user_retention_events.created_at_start_of_hour AS retention_event_ts,
             (retention_event_ts - start_event_ts) / 3600.0 AS ts_delta_hours,
             IF(user_retention_events.user_id > 0, FLOOR(ts_delta_hours / 24.0), -1) AS retention_day
      FROM users_with_start_events
      LEFT JOIN user_retention_events USING (user_id,
                                             platform)),
        uniq_retention_days AS
     (SELECT DISTINCT retention_day
      FROM base_retention),
        uniq_users AS
     (SELECT retention_day,
             COUNT(DISTINCT CASE
                                WHEN retention_day <= maximum_possible_return_day THEN user_id
                            END) AS users,
             COUNT(DISTINCT CASE
                                WHEN platform = 'ANDROID'
                                     AND retention_day <= maximum_possible_return_day THEN user_id
                            END) AS android_users,
             COUNT(DISTINCT CASE
                                WHEN platform = 'IOS'
                                     AND retention_day <= maximum_possible_return_day THEN user_id
                            END) AS ios_users
      FROM uniq_retention_days,
           user_maximum_possible_return_day
      GROUP BY retention_day) SELECT base_retention.start_event_day AS start_event_day,
                                     base_retention.platform AS platform,
                                     base_retention.role_type AS role_type,
                                     base_retention.retention_day AS retention_day,
                                     base_retention.user_id AS user_id,
                                     uniq_users.users AS users,
                                     uniq_users.android_users AS android_users,
                                     uniq_users.ios_users AS ios_users
   FROM base_retention
   JOIN uniq_users USING (retention_day)) AS virtual_table
WHERE retention_day <= 31
  AND retention_day >= 0
GROUP BY retention_day
ORDER BY "ALL" DESC
LIMIT 10000;