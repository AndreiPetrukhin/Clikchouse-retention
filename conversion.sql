"""
This script is intended to calculate conversion for different events (this order) sequence
jijna masks are used for dinamic filtering
"""
SELECT
  day AS day_date,
  platform,
  user_id,
  role_type,
  user_type,
  step,
  step_name,
  step_conversion
FROM (
    SELECT
        MIN(day_date) AS day,
        platform,
        user_id,
        role_type,
        user_type,
        array(
            1
          , 2
            {% if filter_values('third_event') %}
          , 3
            {% endif %}
            {% if filter_values('fourth_event') %}
          , 4
            {% endif %}
            {% if filter_values('fifth_event') %}
          , 5
            {% endif %}
            {% if filter_values('sixth_event') %}
          , 6
            {% endif %}
            {% if filter_values('seventh_event') %}
          , 7
            {% endif %}
            {% if filter_values('eighth_event') %}
          , 8
            {% endif %}
            {% if filter_values('ninth_event') %}
          , 9
            {% endif %}
          ) AS steps,
        array(
            '01_{{ filter_values('first_event')|join("_") }}'
          , '02_{{ filter_values('second_event')|join("_") }}'
            {% if filter_values('third_event') %}
          , '03_{{ filter_values('third_event')|join("_") }}'
            {% endif %}
            {% if filter_values('fourth_event') %}
          , '04_{{ filter_values('fourth_event')|join("_") }}'
            {% endif %}
            {% if filter_values('fifth_event') %}
          , '05_{{ filter_values('fifth_event')|join("_") }}'
            {% endif %}
            {% if filter_values('sixth_event') %}
          , '06_{{ filter_values('sixth_event')|join("_") }}'
            {% endif %}
            {% if filter_values('seventh_event') %}
          , '07_{{ filter_values('seventh_event')|join("_") }}'
            {% endif %}
            {% if filter_values('eighth_event') %}
          , '08_{{ filter_values('eighth_event')|join("_") }}'
            {% endif %}
            {% if filter_values('ninth_event') %}
          , '09_{{ filter_values('ninth_event')|join("_") }}'
            {% endif %}
          ) AS step_names,
        array(1.0,
            sequenceMatch('(?1).*(?2)')(toDateTime(created_at),
                event IN {{ filter_values('first_event')|where_in }},
                event IN {{ filter_values('second_event')|where_in }}
            )
            {% if filter_values('third_event') %}
            , sequenceMatch('(?1).*(?2).*(?3)')(toDateTime(created_at),
                event IN {{ filter_values('first_event')|where_in }},
                event IN {{ filter_values('second_event')|where_in }},
                event IN {{ filter_values('third_event')|where_in }}
            )
            {% endif %}
            {% if filter_values('fourth_event') %}
            , sequenceMatch('(?1).*(?2).*(?3).*(?4)')(toDateTime(created_at),
                event IN {{ filter_values('first_event')|where_in }},
                event IN {{ filter_values('second_event')|where_in }},
                event IN {{ filter_values('third_event')|where_in }},
                event IN {{ filter_values('fourth_event')|where_in }}
            )
            {% endif %}
            {% if filter_values('fifth_event') %}
            , sequenceMatch('(?1).*(?2).*(?3).*(?4).*(?5)')(toDateTime(created_at),
                event IN {{ filter_values('first_event')|where_in }},
                event IN {{ filter_values('second_event')|where_in }},
                event IN {{ filter_values('third_event')|where_in }},
                event IN {{ filter_values('fourth_event')|where_in }},
                event IN {{ filter_values('fifth_event')|where_in }}
            )
            {% endif %}
            {% if filter_values('sixth_event') %}
            , sequenceMatch('(?1).*(?2).*(?3).*(?4).*(?5).*(?6)')(toDateTime(created_at),
                event IN {{ filter_values('first_event')|where_in }},
                event IN {{ filter_values('second_event')|where_in }},
                event IN {{ filter_values('third_event')|where_in }},
                event IN {{ filter_values('fourth_event')|where_in }},
                event IN {{ filter_values('fifth_event')|where_in }},
                event IN {{ filter_values('sixth_event')|where_in }}
            )
            {% endif %}
            {% if filter_values('seventh_event') %}
            , sequenceMatch('(?1).*(?2).*(?3).*(?4).*(?5).*(?6).*(?7)')(toDateTime(created_at),
                event IN {{ filter_values('first_event')|where_in }},
                event IN {{ filter_values('second_event')|where_in }},
                event IN {{ filter_values('third_event')|where_in }},
                event IN {{ filter_values('fourth_event')|where_in }},
                event IN {{ filter_values('fifth_event')|where_in }},
                event IN {{ filter_values('sixth_event')|where_in }},
                event IN {{ filter_values('seventh_event')|where_in }}
            )
            {% endif %}
            {% if filter_values('eighth_event') %}
            , sequenceMatch('(?1).*(?2).*(?3).*(?4).*(?5).*(?6).*(?7).*(?8)')(toDateTime(created_at),
                event IN {{ filter_values('first_event')|where_in }},
                event IN {{ filter_values('second_event')|where_in }},
                event IN {{ filter_values('third_event')|where_in }},
                event IN {{ filter_values('fourth_event')|where_in }},
                event IN {{ filter_values('fifth_event')|where_in }},
                event IN {{ filter_values('sixth_event')|where_in }},
                event IN {{ filter_values('seventh_event')|where_in }},
                event IN {{ filter_values('eighth_event')|where_in }}
            )
            {% endif %}
            {% if filter_values('ninth_event') %}
            , sequenceMatch('(?1).*(?2).*(?3).*(?4).*(?5).*(?6).*(?7).*(?8).*(?9)')(toDateTime(created_at),
                event IN {{ filter_values('first_event')|where_in }},
                event IN {{ filter_values('second_event')|where_in }},
                event IN {{ filter_values('third_event')|where_in }},
                event IN {{ filter_values('fourth_event')|where_in }},
                event IN {{ filter_values('fifth_event')|where_in }},
                event IN {{ filter_values('sixth_event')|where_in }},
                event IN {{ filter_values('seventh_event')|where_in }},
                event IN {{ filter_values('eighth_event')|where_in }},
                event IN {{ filter_values('ninth_event')|where_in }}
            )
            {% endif %}
        ) AS step_conversions
    FROM scheme_name.events
    WHERE TRUE
    {% if from_dttm is not none and to_dttm is not none %}
    -- first event must be between from_dttm and to_dttm but next events may arrive later
     AND day_date BETWEEN toDate('{{ from_dttm }}') AND (toDate('{{ to_dttm }}') + 60)
    {% endif %}
    GROUP BY user_id, platform, role_type, user_type
    HAVING SUM(
      CASE
        WHEN event IN {{ filter_values('first_event')|where_in }}
        THEN 1 ELSE 0
      END) > 0
) AS user_conversions
ARRAY JOIN steps AS step, step_names AS step_name, step_conversions AS step_conversion