```
/**********************
Purpose: Create hard-coded rules for building descriptions and events
Author: Aaron Wilkowitz
Date Created: 2021-10-20
**********************/

/**********************
0. Summarize by second5 to reduce data volume
**********************/

# Note: this step should narrow down to just the last ~60 minutes or so during pipeline process in production

CREATE OR REPLACE TABLE `hca-data-sandbox.fetal_heartrate.rules_classify_0_summarize_by_second5` AS
SELECT
    subjectid
  , cast((FORMAT_TIMESTAMP('%F %H:%M', a.measurement_timestamp )) || ':' || floor(extract(second from a.measurement_timestamp) / 5)*5 as timestamp) as measurement_timestamp
  , datatype
  , monitorid
  , sensortype
  , case when sensortype in ('INOP','No_Trans') then 'Invalid Data' else 'Valid Data' end as data_quality
  , avg(value) as value
-- SELECT *
FROM `hca-data-sandbox.looker_scratch2.A3_f4_fetal_heartrate_monitoring_fetal_heartrate_monitoring_sample_pre` a
GROUP BY 1,2,3,4,5
;

/**********************
I. Baseline: FHR, UA, US
**********************/

# Granularity
  -- By patient by every five seconds

# Method:
  -- Take last 30 min of data
  -- Remove bad data
  -- Remove top and bottom quartile
  -- Take the average value

CREATE OR REPLACE TABLE `hca-data-sandbox.fetal_heartrate.rules_classify_1_baseline_last_30_min` AS
WITH array_summary_last_30_min as
  (
    # 2. Order that array by values
    SELECT * except (array_window), ARRAY(SELECT x FROM UNNEST(array_window) AS x ORDER BY x) as array_window
    FROM
    (
      SELECT
          *
        # 1. Create an array of last 360 values (30 minutes * 60 sec/min / 5 second snippets)
        , ARRAY_AGG(value) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp ROWS BETWEEN 360 PRECEDING AND 0 FOLLOWING) as array_window
      FROM `hca-data-sandbox.fetal_heartrate.rules_classify_0_summarize_by_second5`
    ) a
  )
, quartile_1_3 as
(
SELECT
    *
    # 3. Set 1st quartile and 3rd quartile
  , array_window[OFFSET(cast(floor((array_length(array_window) / 100) * 25) as int64))] as quartile_1
  , array_window[OFFSET(cast(floor((array_length(array_window) / 100) * 75) as int64))] as quartile_3
FROM array_summary_last_30_min a
)
SELECT
    a.measurement_timestamp
  , a.subjectid
  , a.datatype
  , a.data_quality
  , a.value
# 4. Take average of values between 1st and 3rd quartile
  , (SELECT avg(x) FROM UNNEST(a.array_window) x WHERE x >= quartile_1 and x <= quartile_3) as baseline
FROM quartile_1_3 a
;

/**********************
II. Variability: FHR
**********************/

# Granularity
  -- By patient by every five seconds

# Method:
  -- Take last 3 min of data
  -- Take the average of the absolute difference between every point and the 5 seconds before it

CREATE OR REPLACE TABLE `hca-data-sandbox.fetal_heartrate.rules_classify_2_variability_last_3_min` AS
with diff_last_value AS
(
SELECT
    # 1. Calculate absolute difference between every point and 5 seconds before it
    *
  , LAST_VALUE(value) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS last_value
  , abs(value - LAST_VALUE(value) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING)) AS abs_difference
FROM `hca-data-sandbox.fetal_heartrate.rules_classify_1_baseline_last_30_min`
)
SELECT
    a.measurement_timestamp
  , a.subjectid
  , a.datatype
  , a.data_quality
  , a.value
  , baseline
    # 2. Calculate average of those differences over last 3 minutes (12 readings / minute (every 5 seconds) * 3 minutes)
  , AVG(abs_difference) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp ROWS BETWEEN 36 PRECEDING AND 0 FOLLOWING) as variability
FROM diff_last_value a
;

/**********************
III. List out All Event:
  Deceleration
  Acceleration
  Contraction
  Uterine Stimulation
**********************/

# Granularity
  -- By patient by every five seconds

# Method:
  -- Start: 15 x 15 - difference of 15 bpm for at least 15 seconds (3 observations)
  -- End: 0 x 15 - at least 15 seconds of back to baseline measurements

CREATE OR REPLACE TABLE `hca-data-sandbox.fetal_heartrate.rules_classify_3_events_part1` AS
with is_event as (
  SELECT
  *
  , case when datatype in ('HR1','HR2') and value - baseline <= -15 then 1.0 else 0 end as is_change1 -- Dec
  , case when datatype in ('HR1','HR2') and value - baseline >= 15 then 1.0 else 0 end as is_change2 -- Acc
  , case when datatype in ('UA') and value - baseline >= 15 then 1.0 else 0 end as is_change3 -- Contract
  , case when datatype in ('US') and value - baseline >= 15 then 1.0 else 0 end as is_change4 -- Uterine Stim
  FROM `hca-data-sandbox.fetal_heartrate.rules_classify_2_variability_last_3_min`
),
is_event_last_x_seconds as (
  SELECT
      *
    , AVG(is_change1) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp ROWS BETWEEN 2 PRECEDING AND 0 PRECEDING) AS last_x_seconds1 -- Dec
    , AVG(is_change2) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp ROWS BETWEEN 2 PRECEDING AND 0 PRECEDING) AS last_x_seconds2 -- Acc
    , AVG(is_change3) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp ROWS BETWEEN 2 PRECEDING AND 0 PRECEDING) AS last_x_seconds3 -- Contract
    , AVG(is_change4) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp ROWS BETWEEN 2 PRECEDING AND 0 PRECEDING) AS last_x_seconds4 -- Uterine Stim
  FROM is_event
),
period_state_every_y_seconds as (
  SELECT
      *
    , case when last_x_seconds1 = 0 then 'End of Period' when last_x_seconds1 = 1 then 'Start of Period' else 'Unknown' end as period_state1 -- Dec
    , case when last_x_seconds2 = 0 then 'End of Period' when last_x_seconds2 = 1 then 'Start of Period' else 'Unknown' end as period_state2 -- Acc
    , case when last_x_seconds3 = 0 then 'End of Period' when last_x_seconds3 = 1 then 'Start of Period' else 'Unknown' end as period_state3 -- Contract
    , case when last_x_seconds4 = 0 then 'End of Period' when last_x_seconds4 = 1 then 'Start of Period' else 'Unknown' end as period_state4 -- Uterine Stim
  FROM is_event_last_x_seconds
),
period_state_every_y_seconds_and_prior as (
SELECT
    *
  , coalesce(last_value(period_state1) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING),'Unknown') AS period_state_prior1 -- Dec
  , coalesce(last_value(period_state2) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING),'Unknown') AS period_state_prior2 -- Acc
  , coalesce(last_value(period_state3) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING),'Unknown') AS period_state_prior3 -- Contract
  , coalesce(last_value(period_state4) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING),'Unknown') AS period_state_prior4 -- Uterine Stim
FROM period_state_every_y_seconds
) ,
period_state_every_y_seconds_and_prior_summary_pre as (
  SELECT
      *
    , case
        when period_state1 = 'End of Period'    and period_state_prior1 = 'End of Period' then 'Not Event'
        when period_state1 = 'End of Period'    and period_state_prior1 = 'Unknown' then 'Not Event'
        when period_state1 = 'Unknown'          and period_state_prior1 = 'End of Period' then 'Not Event'
        when period_state1 = 'Start of Period'  and period_state_prior1 = 'Start of Period' then 'Event'
        when period_state1 = 'Start of Period'  and period_state_prior1 = 'Unknown' then 'Event'
        when period_state1 = 'Unknown'          and period_state_prior1 = 'Start of Period' then 'Event'
        when period_state1 = 'Unknown'          and period_state_prior1 = 'Unknown' then NULL
        else NULL
      end as is_event_pre1 -- Dec
    , case
        when period_state2 = 'End of Period'    and period_state_prior2 = 'End of Period' then 'Not Event'
        when period_state2 = 'End of Period'    and period_state_prior2 = 'Unknown' then 'Not Event'
        when period_state2 = 'Unknown'          and period_state_prior2 = 'End of Period' then 'Not Event'
        when period_state2 = 'Start of Period'  and period_state_prior2 = 'Start of Period' then 'Event'
        when period_state2 = 'Start of Period'  and period_state_prior2 = 'Unknown' then 'Event'
        when period_state2 = 'Unknown'          and period_state_prior2 = 'Start of Period' then 'Event'
        when period_state2 = 'Unknown'          and period_state_prior2 = 'Unknown' then NULL
        else NULL
      end as is_event_pre2 -- Acc
    , case
        when period_state3 = 'End of Period'    and period_state_prior3 = 'End of Period' then 'Not Event'
        when period_state3 = 'End of Period'    and period_state_prior3 = 'Unknown' then 'Not Event'
        when period_state3 = 'Unknown'          and period_state_prior3 = 'End of Period' then 'Not Event'
        when period_state3 = 'Start of Period'  and period_state_prior3 = 'Start of Period' then 'Event'
        when period_state3 = 'Start of Period'  and period_state_prior3 = 'Unknown' then 'Event'
        when period_state3 = 'Unknown'          and period_state_prior3 = 'Start of Period' then 'Event'
        when period_state3 = 'Unknown'          and period_state_prior3 = 'Unknown' then NULL
        else NULL
      end as is_event_pre3 -- Contract
    , case
        when period_state4 = 'End of Period'    and period_state_prior4 = 'End of Period' then 'Not Event'
        when period_state4 = 'End of Period'    and period_state_prior4 = 'Unknown' then 'Not Event'
        when period_state4 = 'Unknown'          and period_state_prior4 = 'End of Period' then 'Not Event'
        when period_state4 = 'Start of Period'  and period_state_prior4 = 'Start of Period' then 'Event'
        when period_state4 = 'Start of Period'  and period_state_prior4 = 'Unknown' then 'Event'
        when period_state4 = 'Unknown'          and period_state_prior4 = 'Start of Period' then 'Event'
        when period_state4 = 'Unknown'          and period_state_prior4 = 'Unknown' then NULL
        else NULL
      end as is_event_pre4 -- Uterine Stim
  FROM period_state_every_y_seconds_and_prior
),
period_state_every_y_seconds_and_prior_summary as (
SELECT
    *
  , last_value(is_event_pre1 IGNORE NULLS) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp ROWS BETWEEN 200 PRECEDING AND 0 PRECEDING) as is_event1 -- Dec
  , last_value(is_event_pre2 IGNORE NULLS) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp ROWS BETWEEN 200 PRECEDING AND 0 PRECEDING) as is_event2 -- Acc
  , last_value(is_event_pre3 IGNORE NULLS) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp ROWS BETWEEN 200 PRECEDING AND 0 PRECEDING) as is_event3 -- Contract
  , last_value(is_event_pre4 IGNORE NULLS) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp ROWS BETWEEN 200 PRECEDING AND 0 PRECEDING) as is_event4 -- Uterine Stim
FROM period_state_every_y_seconds_and_prior_summary_pre
),
period_state_every_y_seconds_and_prior_summary_prior as (
  SELECT
      *
    , last_value(is_event1 IGNORE NULLS) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) as is_event_prior1 -- Dec
    , last_value(is_event2 IGNORE NULLS) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) as is_event_prior2 -- Acc
    , last_value(is_event3 IGNORE NULLS) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) as is_event_prior3 -- Contract
    , last_value(is_event4 IGNORE NULLS) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) as is_event_prior4 -- Uterine Stim
  FROM period_state_every_y_seconds_and_prior_summary
)
SELECT
    *
  , case when is_event1 = 'Event' and is_event_prior1 = 'Not Event' then 'Event Start' when is_event1 = 'Not Event' and is_event_prior1 = 'Event' then 'Event End' end as event_start_end1 -- Dec
  , case when is_event2 = 'Event' and is_event_prior2 = 'Not Event' then 'Event Start' when is_event2 = 'Not Event' and is_event_prior2 = 'Event' then 'Event End' end as event_start_end2 -- Acc
  , case when is_event3 = 'Event' and is_event_prior3 = 'Not Event' then 'Event Start' when is_event3 = 'Not Event' and is_event_prior3 = 'Event' then 'Event End' end as event_start_end3 -- Contract
  , case when is_event4 = 'Event' and is_event_prior4 = 'Not Event' then 'Event Start' when is_event4 = 'Not Event' and is_event_prior4 = 'Event' then 'Event End' end as event_start_end4 -- Uterine Stim
FROM period_state_every_y_seconds_and_prior_summary_prior
;

CREATE OR REPLACE TABLE `hca-data-sandbox.fetal_heartrate.rules_classify_3_events_part2` AS
with period_state_every_y_seconds_event_id as (
  SELECT
      a.*
    , b.event_id as event_id1 -- Dec
    , c.event_id as event_id2 -- Acc
    , d.event_id as event_id3 -- Contract
    , e.event_id as event_id4 -- Uterine Stim
  FROM `hca-data-sandbox.fetal_heartrate.rules_classify_3_events_part1` a
  LEFT JOIN
  (
    SELECT
        measurement_timestamp
      , subjectid
      , datatype
      , rank() over (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp) as event_id
    FROM `hca-data-sandbox.fetal_heartrate.rules_classify_3_events_part1`
    WHERE event_start_end1 = 'Event Start'
    UNION ALL
    SELECT
        measurement_timestamp
      , subjectid
      , datatype
      , rank() over (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp) as event_id
    FROM `hca-data-sandbox.fetal_heartrate.rules_classify_3_events_part1`
    WHERE event_start_end1 = 'Event End'
  ) b
    ON a.measurement_timestamp = b.measurement_timestamp
    AND a.subjectid = b.subjectid
    AND a.datatype = b.datatype
  LEFT JOIN
  (
    SELECT
        measurement_timestamp
      , subjectid
      , datatype
      , rank() over (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp) as event_id
    FROM `hca-data-sandbox.fetal_heartrate.rules_classify_3_events_part1`
    WHERE event_start_end2 = 'Event Start'
    UNION ALL
    SELECT
        measurement_timestamp
      , subjectid
      , datatype
      , rank() over (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp) as event_id
    FROM `hca-data-sandbox.fetal_heartrate.rules_classify_3_events_part1`
    WHERE event_start_end2 = 'Event End'
  ) c
    ON a.measurement_timestamp = c.measurement_timestamp
    AND a.subjectid = c.subjectid
    AND a.datatype = c.datatype
  LEFT JOIN
  (
    SELECT
        measurement_timestamp
      , subjectid
      , datatype
      , rank() over (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp) as event_id
    FROM `hca-data-sandbox.fetal_heartrate.rules_classify_3_events_part1`
    WHERE event_start_end3 = 'Event Start'
    UNION ALL
    SELECT
        measurement_timestamp
      , subjectid
      , datatype
      , rank() over (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp) as event_id
    FROM `hca-data-sandbox.fetal_heartrate.rules_classify_3_events_part1`
    WHERE event_start_end3 = 'Event End'
  ) d
    ON a.measurement_timestamp = d.measurement_timestamp
    AND a.subjectid = d.subjectid
    AND a.datatype = d.datatype
  LEFT JOIN
  (
    SELECT
        measurement_timestamp
      , subjectid
      , datatype
      , rank() over (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp) as event_id
    FROM `hca-data-sandbox.fetal_heartrate.rules_classify_3_events_part1`
    WHERE event_start_end4 = 'Event Start'
    UNION ALL
    SELECT
        measurement_timestamp
      , subjectid
      , datatype
      , rank() over (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp) as event_id
    FROM `hca-data-sandbox.fetal_heartrate.rules_classify_3_events_part1`
    WHERE event_start_end4 = 'Event End'
  ) e
    ON a.measurement_timestamp = e.measurement_timestamp
    AND a.subjectid = e.subjectid
    AND a.datatype = e.datatype
),
pre_summary as (
  SELECT
        a.* except (event_id1, event_id2, event_id3, event_id4)
      , b.event_id1 -- Dec
      , c.event_id2 -- Acc
      , d.event_id3 -- Contract
      , e.event_id4 -- Uterine Stim
    FROM period_state_every_y_seconds_event_id a
    LEFT JOIN
    (
      SELECT
            measurement_timestamp
          , subjectid
          , datatype
          , event_id1
      FROM period_state_every_y_seconds_event_id
      WHERE is_event1 = 'Not Event'
      UNION ALL
      SELECT
          a.measurement_timestamp
        , a.subjectid
        , a.datatype
        , b.event_id1
      FROM period_state_every_y_seconds_event_id a
      CROSS JOIN
      (
        SELECT subjectid, datatype, event_id1, min(measurement_timestamp) as starttime, max(measurement_timestamp) as endtime
        FROM period_state_every_y_seconds_event_id
        GROUP BY 1,2,3
      ) b
      WHERE a.measurement_timestamp BETWEEN b.starttime and b.endtime
      AND a.subjectid = b.subjectid
      AND a.datatype = b.datatype
      AND a.is_event1 = 'Event'
      AND b.event_id1 is not null
    ) b
      ON a.measurement_timestamp = b.measurement_timestamp
      AND a.subjectid = b.subjectid
      AND a.datatype = b.datatype
    LEFT JOIN
    (
      SELECT
            measurement_timestamp
          , subjectid
          , datatype
          , event_id2
      FROM period_state_every_y_seconds_event_id
      WHERE is_event2 = 'Not Event'
      UNION ALL
      SELECT
          a.measurement_timestamp
        , a.subjectid
        , a.datatype
        , b.event_id2
      FROM period_state_every_y_seconds_event_id a
      CROSS JOIN
      (
        SELECT subjectid, datatype, event_id2, min(measurement_timestamp) as starttime, max(measurement_timestamp) as endtime
        FROM period_state_every_y_seconds_event_id
        GROUP BY 1,2,3
      ) b
      WHERE a.measurement_timestamp BETWEEN b.starttime and b.endtime
      AND a.subjectid = b.subjectid
      AND a.datatype = b.datatype
      AND a.is_event2 = 'Event'
      AND b.event_id2 is not null
    ) c
      ON a.measurement_timestamp = c.measurement_timestamp
      AND a.subjectid = c.subjectid
      AND a.datatype = c.datatype
    LEFT JOIN
    (
      SELECT
            measurement_timestamp
          , subjectid
          , datatype
          , event_id3
      FROM period_state_every_y_seconds_event_id
      WHERE is_event3 = 'Not Event'
      UNION ALL
      SELECT
          a.measurement_timestamp
        , a.subjectid
        , a.datatype
        , b.event_id3
      FROM period_state_every_y_seconds_event_id a
      CROSS JOIN
      (
        SELECT subjectid, datatype, event_id3, min(measurement_timestamp) as starttime, max(measurement_timestamp) as endtime
        FROM period_state_every_y_seconds_event_id
        GROUP BY 1,2,3
      ) b
      WHERE a.measurement_timestamp BETWEEN b.starttime and b.endtime
      AND a.subjectid = b.subjectid
      AND a.datatype = b.datatype
      AND a.is_event3 = 'Event'
      AND b.event_id3 is not null
    ) d
      ON a.measurement_timestamp = d.measurement_timestamp
      AND a.subjectid = d.subjectid
      AND a.datatype = d.datatype
    LEFT JOIN
    (
      SELECT
            measurement_timestamp
          , subjectid
          , datatype
          , event_id4
      FROM period_state_every_y_seconds_event_id
      WHERE is_event4 = 'Not Event'
      UNION ALL
      SELECT
          a.measurement_timestamp
        , a.subjectid
        , a.datatype
        , b.event_id4
      FROM period_state_every_y_seconds_event_id a
      CROSS JOIN
      (
        SELECT subjectid, datatype, event_id4, min(measurement_timestamp) as starttime, max(measurement_timestamp) as endtime
        FROM period_state_every_y_seconds_event_id
        GROUP BY 1,2,3
      ) b
      WHERE a.measurement_timestamp BETWEEN b.starttime and b.endtime
      AND a.subjectid = b.subjectid
      AND a.datatype = b.datatype
      AND a.is_event4 = 'Event'
      AND b.event_id4 is not null
    ) e
      ON a.measurement_timestamp = e.measurement_timestamp
      AND a.subjectid = e.subjectid
      AND a.datatype = e.datatype
)
  SELECT *
  FROM pre_summary
;

CREATE OR REPLACE TABLE `hca-data-sandbox.fetal_heartrate.rules_classify_3_events_part3` AS
SELECT
  distinct
    a.measurement_timestamp
  , a.subjectid
  , b.value             as ehr_value
  , c.value             as ua_value
  , d.value             as us_value
  , b.is_event1         as is_deceleration
  , b.event_start_end1  as deceleration_start_end
  , b.event_id1         as deceleration_event_id
  , b.is_event2         as is_acceleration
  , b.event_start_end2  as acceleration_start_end
  , b.event_id2         as acceleration_event_id
  , c.is_event3         as is_contraction
  , c.event_start_end3  as contraction_start_end
  , c.event_id3         as contraction_event_id
  , d.is_event4         as is_uterine_stimulation
  , d.event_start_end4  as uterine_stimulation_start_end
  , d.event_id4         as uterine_stimulation_event_id
FROM `hca-data-sandbox.fetal_heartrate.rules_classify_3_events_part2` a
LEFT JOIN
(
  SELECT *
  FROM `hca-data-sandbox.fetal_heartrate.rules_classify_3_events_part2`
  WHERE datatype in ('HR1','HR2')
) b
  ON a.measurement_timestamp = b.measurement_timestamp
  AND a.subjectid = b.subjectid
LEFT JOIN
(
  SELECT *
  FROM `hca-data-sandbox.fetal_heartrate.rules_classify_3_events_part2`
  WHERE datatype in ('UA')
) c
  ON a.measurement_timestamp = c.measurement_timestamp
  AND a.subjectid = c.subjectid
LEFT JOIN
(
  SELECT *
  FROM `hca-data-sandbox.fetal_heartrate.rules_classify_3_events_part2`
  WHERE datatype in ('US')
) d
  ON a.measurement_timestamp = d.measurement_timestamp
  AND a.subjectid = d.subjectid
;

/**********************
IV. List out Event Summary: Decelerations
  -- By patientID, by event
  -- KPIs related to the Events
**********************/

-- Dec
  -- start
  -- end
  -- length
  -- amplitude
  -- nearest contraction
  -- diff in start
  -- diff in end
  -- # overlap
  -- % overlap

CREATE OR REPLACE TABLE `hca-data-sandbox.fetal_heartrate.rules_classify_4_deceleration` AS
with decelerations as
(
  SELECT
      subjectid
    , deceleration_event_id
    , min(measurement_timestamp) as deceleration_start_time
    , max(measurement_timestamp) as deceleration_end_time
    , count(distinct measurement_timestamp) * 5 as deceleration_length
  FROM `hca-data-sandbox.fetal_heartrate.rules_classify_3_events_part3`
  WHERE deceleration_event_id is not null
  GROUP BY 1,2
),
dec_amplitude as
(
  SELECT
      b.subjectid
    , b.deceleration_event_id
    , max(abs(a.value - a.baseline)) as amplitude
  FROM `hca-data-sandbox.fetal_heartrate.rules_classify_2_variability_last_3_min` a
  CROSS JOIN decelerations b
  WHERE a.subjectid = b.subjectid
  AND a.measurement_timestamp >= b.deceleration_start_time
  AND a.measurement_timestamp <= b.deceleration_end_time
  GROUP BY 1,2
),
contractions as
(
  SELECT
      subjectid
    , contraction_event_id
    , min(measurement_timestamp) as contraction_start_time
    , max(measurement_timestamp) as contraction_end_time
    , count(distinct measurement_timestamp) * 5 as length_seconds
  FROM `hca-data-sandbox.fetal_heartrate.rules_classify_3_events_part3`
  WHERE contraction_event_id is not null
  GROUP BY 1,2
),
cross_join_dec_con as
(
  SELECT
      a.subjectid
    , a.deceleration_event_id
    , b.contraction_event_id
    , abs(timestamp_diff(a.deceleration_start_time, b.contraction_start_time, second)) as start_time_diff
    , abs(timestamp_diff(a.deceleration_end_time, b.contraction_end_time, second)) as end_time_diff
  FROM decelerations a
  LEFT JOIN contractions b
    ON a.subjectid = b.subjectid
    AND abs(timestamp_diff(a.deceleration_start_time, b.contraction_start_time, minute)) < 15
    AND abs(timestamp_diff(a.deceleration_end_time, b.contraction_end_time, minute)) < 15
),
cross_join_dec_con_average as
(
  SELECT
      subjectid
    , deceleration_event_id
    , contraction_event_id
    , (start_time_diff + end_time_diff) / 2 as avg_time_diff
  FROM cross_join_dec_con
),
dec_con_mapping as
(
  SELECT
      a.subjectid
    , a.deceleration_event_id
    , min(a.contraction_event_id) as contraction_event_id
  FROM cross_join_dec_con_average a
  LEFT JOIN
  (
    SELECT
        subjectid
      , deceleration_event_id
      , min(avg_time_diff) as avg_time_diff
    FROM cross_join_dec_con_average
    GROUP BY 1,2
  ) b
    ON a.subjectid = b.subjectid
    AND a.deceleration_event_id = b.deceleration_event_id
    AND a.avg_time_diff = b.avg_time_diff
  GROUP BY 1,2
),
dec_con_ordering as
(
  SELECT
      a.*
    , c.contraction_start_time
    , c.contraction_end_time
    , c.length_seconds as contraction_length
    , timestamp_diff(a.deceleration_start_time, c.contraction_start_time, second) as start_time_diff
    , timestamp_diff(a.deceleration_end_time, c.contraction_end_time, second) as end_time_diff
  FROM decelerations a
  LEFT JOIN dec_con_mapping b
    ON a.subjectid = b.subjectid
    AND a.deceleration_event_id = b.deceleration_event_id
  LEFT JOIN contractions c
    ON a.subjectid = c.subjectid
    AND b.contraction_event_id = c.contraction_event_id
),
dec_con_ordering_type as
(
  SELECT
    *,
      case
        when start_time_diff >= 0 and end_time_diff >= 0 then 'p-p'
        when start_time_diff <= 0 and end_time_diff >= 0 then 'n-p'
        when start_time_diff >= 0 and end_time_diff <= 0 then 'p-n'
        when start_time_diff <= 0 and end_time_diff <= 0 then 'n-n'
      end as type
  FROM dec_con_ordering
)
  SELECT
      a.subjectid
    , a.deceleration_start_time
    , a.deceleration_end_time
    , a.deceleration_length
    , b.amplitude as deceleration_amplitude
    , a.contraction_start_time as matching_contraction_start
    , a.contraction_end_time as matching_contraction_end
    , a.contraction_length as matching_contraction_length
    , a.start_time_diff
    , a.end_time_diff
    , type
    , case
        when type = 'n-p' then contraction_length
        when type = 'p-n' then deceleration_length
        when type = 'n-n' and deceleration_length - abs(start_time_diff) < 0 then 0
        when type = 'n-n' and deceleration_length - abs(start_time_diff) >= 0 then deceleration_length - abs(start_time_diff)
        when type = 'p-p' and deceleration_length - abs(end_time_diff) < 0 then 0
        when type = 'p-p' and deceleration_length - abs(end_time_diff) >= 0 then deceleration_length - abs(end_time_diff)
      end
      as seconds_overlap
  FROM dec_con_ordering_type a
  LEFT JOIN dec_amplitude b
    ON a.subjectid = b.subjectid
    AND a.deceleration_event_id = b.deceleration_event_id
;

-- Contraction
  -- start
  -- end
  -- length
  -- amplitude

CREATE OR REPLACE TABLE `hca-data-sandbox.fetal_heartrate.rules_classify_4_contraction` AS
with contractions as
(
  SELECT
      subjectid
    , contraction_event_id
    , min(measurement_timestamp) as contraction_start_time
    , max(measurement_timestamp) as contraction_end_time
    , count(distinct measurement_timestamp) * 5 as contraction_length
  FROM `hca-data-sandbox.fetal_heartrate.rules_classify_3_events_part3`
  WHERE contraction_event_id is not null
  GROUP BY 1,2
),
con_amplitude as
(
  SELECT
      b.subjectid
    , b.contraction_event_id
    , max(abs(a.value - a.baseline)) as amplitude
  FROM `hca-data-sandbox.fetal_heartrate.rules_classify_2_variability_last_3_min` a
  CROSS JOIN contractions b
  WHERE a.subjectid = b.subjectid
  AND a.measurement_timestamp >= b.contraction_start_time
  AND a.measurement_timestamp <= b.contraction_end_time
  GROUP BY 1,2
)
  SELECT
      a.*
    , b.amplitude
  FROM contractions a
  LEFT JOIN con_amplitude b
    ON a.subjectid = b.subjectid
    AND a.contraction_event_id = b.contraction_event_id
;

-- Uterine stimulation
  -- start
  -- end
  -- length
  -- amplitude
  -- nearest acceleration
  -- diff in start
  -- diff in end
  -- # overlap

CREATE OR REPLACE TABLE `hca-data-sandbox.fetal_heartrate.rules_classify_4_uterine_stimulation` AS
with uterine_stimulation as
(
  SELECT
      subjectid
    , uterine_stimulation_event_id
    , min(measurement_timestamp) as uterine_stimulation_start_time
    , max(measurement_timestamp) as uterine_stimulation_end_time
    , count(distinct measurement_timestamp) * 5 as uterine_stimulation_length
  FROM `hca-data-sandbox.fetal_heartrate.rules_classify_3_events_part3`
  WHERE uterine_stimulation_event_id is not null
  GROUP BY 1,2
),
us_amplitude as
(
  SELECT
      b.subjectid
    , b.uterine_stimulation_event_id
    , max(abs(a.value - a.baseline)) as amplitude
  FROM `hca-data-sandbox.fetal_heartrate.rules_classify_2_variability_last_3_min` a
  CROSS JOIN uterine_stimulation b
  WHERE a.subjectid = b.subjectid
  AND a.measurement_timestamp >= b.uterine_stimulation_start_time
  AND a.measurement_timestamp <= b.uterine_stimulation_end_time
  GROUP BY 1,2
),
acceleration as
(
  SELECT
      subjectid
    , acceleration_event_id
    , min(measurement_timestamp) as acceleration_start_time
    , max(measurement_timestamp) as acceleration_end_time
    , count(distinct measurement_timestamp) * 5 as length_seconds
  FROM `hca-data-sandbox.fetal_heartrate.rules_classify_3_events_part3`
  WHERE acceleration_event_id is not null
  GROUP BY 1,2
),
cross_join_us_acc as
(
  SELECT
      a.subjectid
    , a.uterine_stimulation_event_id
    , b.acceleration_event_id
    , abs(timestamp_diff(a.uterine_stimulation_start_time, b.acceleration_start_time, second)) as start_time_diff
    , abs(timestamp_diff(a.uterine_stimulation_end_time,   b.acceleration_end_time, second)) as end_time_diff
  FROM uterine_stimulation a
  LEFT JOIN acceleration b
    ON a.subjectid = b.subjectid
    AND abs(timestamp_diff(a.uterine_stimulation_start_time, b.acceleration_start_time, minute)) < 15
    AND abs(timestamp_diff(a.uterine_stimulation_end_time,   b.acceleration_end_time, minute)) < 15
),
cross_join_us_acc_average as
(
  SELECT
      subjectid
    , uterine_stimulation_event_id
    , acceleration_event_id
    , (start_time_diff + end_time_diff) / 2 as avg_time_diff
  FROM cross_join_us_acc
),
us_acc_mapping as
(
  SELECT
      a.subjectid
    , a.uterine_stimulation_event_id
    , min(a.acceleration_event_id) as acceleration_event_id
  FROM cross_join_us_acc_average a
  LEFT JOIN
  (
    SELECT
        subjectid
      , uterine_stimulation_event_id
      , min(avg_time_diff) as avg_time_diff
    FROM cross_join_us_acc_average
    GROUP BY 1,2
  ) b
    ON a.subjectid = b.subjectid
    AND a.uterine_stimulation_event_id = b.uterine_stimulation_event_id
    AND a.avg_time_diff = b.avg_time_diff
  GROUP BY 1,2
),
us_acc_ordering as
(
  SELECT
      a.*
    , c.acceleration_start_time
    , c.acceleration_end_time
    , c.length_seconds as acceleration_length
    , timestamp_diff(a.uterine_stimulation_start_time, c.acceleration_start_time, second) as start_time_diff
    , timestamp_diff(a.uterine_stimulation_end_time,   c.acceleration_end_time, second) as end_time_diff
  FROM uterine_stimulation a
  LEFT JOIN us_acc_mapping b
    ON a.subjectid = b.subjectid
    AND a.uterine_stimulation_event_id = b.uterine_stimulation_event_id
  LEFT JOIN acceleration c
    ON a.subjectid = c.subjectid
    AND b.acceleration_event_id = c.acceleration_event_id
),
us_acc_ordering_type as
(
  SELECT
    *,
      case
        when start_time_diff >= 0 and end_time_diff >= 0 then 'p-p'
        when start_time_diff <= 0 and end_time_diff >= 0 then 'n-p'
        when start_time_diff >= 0 and end_time_diff <= 0 then 'p-n'
        when start_time_diff <= 0 and end_time_diff <= 0 then 'n-n'
      end as type
  FROM us_acc_ordering
)
  SELECT
      a.subjectid
    , a.uterine_stimulation_start_time
    , a.uterine_stimulation_end_time
    , a.uterine_stimulation_length
    , b.amplitude as deceleration_amplitude
    , a.acceleration_start_time as  matching_acceleration_start
    , a.acceleration_end_time as    matching_acceleration_end
    , a.acceleration_length as      matching_acceleration_length
    , a.start_time_diff
    , a.end_time_diff
    , type
    , case
        when type = 'n-p' then acceleration_length
        when type = 'p-n' then uterine_stimulation_length
        when type = 'n-n' and  uterine_stimulation_length - abs(start_time_diff) < 0 then 0
        when type = 'n-n' and  uterine_stimulation_length - abs(start_time_diff) >= 0 then uterine_stimulation_length - abs(start_time_diff)
        when type = 'p-p' and  uterine_stimulation_length - abs(end_time_diff) < 0 then 0
        when type = 'p-p' and  uterine_stimulation_length - abs(end_time_diff) >= 0 then   uterine_stimulation_length - abs(end_time_diff)
      end
      as seconds_overlap
  FROM us_acc_ordering_type a
  LEFT JOIN us_amplitude b
    ON a.subjectid = b.subjectid
    AND a.uterine_stimulation_event_id = b.uterine_stimulation_event_id
;

-- Acceleration
  -- start
  -- end
  -- length
  -- amplitude

CREATE OR REPLACE TABLE `hca-data-sandbox.fetal_heartrate.rules_classify_4_acceleration` AS
with acceleration as
(
  SELECT
      subjectid
    , acceleration_event_id
    , min(measurement_timestamp) as acceleration_start_time
    , max(measurement_timestamp) as acceleration_end_time
    , count(distinct measurement_timestamp) * 5 as acceleration_length
  FROM `hca-data-sandbox.fetal_heartrate.rules_classify_3_events_part3`
  WHERE acceleration_event_id is not null
  GROUP BY 1,2
),
acc_amplitude as
(
  SELECT
      b.subjectid
    , b.acceleration_event_id
    , max(abs(a.value - a.baseline)) as amplitude
  FROM `hca-data-sandbox.fetal_heartrate.rules_classify_2_variability_last_3_min` a
  CROSS JOIN acceleration b
  WHERE a.subjectid = b.subjectid
  AND a.measurement_timestamp >= b.acceleration_start_time
  AND a.measurement_timestamp <= b.acceleration_end_time
  GROUP BY 1,2
)
  SELECT
      a.*
    , b.amplitude
  FROM acceleration a
  LEFT JOIN acc_amplitude b
    ON a.subjectid = b.subjectid
    AND a.acceleration_event_id = b.acceleration_event_id
    ORDER BY 1,2
;

/**********************
V. Combine into Summary Table with Structs
**********************/

SELECT a.* except (last_10_minutes_measurements)
  , ARRAY_AGG(struct(a.measurement_timestamp, a.value)) OVER (PARTITION BY a.subjectid, a.datatype ORDER BY a.measurement_timestamp ROWS BETWEEN 120 PRECEDING AND 0 FOLLOWING) as last_10_minutes_measurements
  , ARRAY_AGG(struct(b.deceleration_start_time, b.deceleration_end_time)) as decelerations

SELECT
  a.*
FROM `hca-data-sandbox.fetal_heartrate.rules_classify_2a_insert_in_last_10_min_array` a
LEFT JOIN
(
  SELECT
      a.measurement_timestamp
    , a.subjectid
    , b.deceleration_start_time
    , b.deceleration_end_time
    , b.deceleration_length
    , b.deceleration_amplitude
    , b.matching_contraction_start
    , b.matching_contraction_end
    , b.matching_contraction_length
    , b.start_time_diff
    , b.end_time_diff
    , b.type
    , b.seconds_overlap
FROM `hca-data-sandbox.fetal_heartrate.rules_classify_2a_insert_in_last_10_min_array` a
CROSS JOIN `hca-data-sandbox.fetal_heartrate.rules_classify_4_deceleration` b
WHERE a.subjectid = b.subjectid
AND a.measurement_timestamp BETWEEN b.deceleration_start_time AND timestamp_add(b.deceleration_start_time, interval 30 minute)
) b
  ON a.measurement_timestamp = b.measurement_timestamp
  AND a.subjectid = b.subjectid
GROUP BY 1,2,3,4,5,6,7
ORDER BY 2,1

-- CREATE OR REPLACE TABLE `hca-data-sandbox.fetal_heartrate.rules_classify_2a_insert_in_last_10_min_array` AS
-- SELECT
--     *
--   , ARRAY_AGG(struct(measurement_timestamp, value)) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp ROWS BETWEEN 120 PRECEDING AND 0 FOLLOWING) as last_10_minutes_measurements
-- FROM `hca-data-sandbox.fetal_heartrate.rules_classify_2_variability_last_3_min`
-- ;
```
