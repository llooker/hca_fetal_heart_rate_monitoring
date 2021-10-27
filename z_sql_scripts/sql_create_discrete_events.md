```
/**********************
Purpose: Create hard-coded rules for building descriptions and events
Author: Aaron Wilkowitz
Date Created: 2021-10-20
**********************/

/**********************
0. Summarize by second5 to reduce data volume
**********************/

# Create a union all table

CREATE OR REPLACE TABLE `hca-data-sandbox.fetal_heartrate.rules_classify_0_union_all_input_data` AS
SELECT * FROM `hca-data-sandbox.looker_scratch2.A3_f4_fetal_heartrate_monitoring_fetal_heartrate_monitoring_sample_pre`
UNION ALL
SELECT * FROM `hca-data-sandbox.fetal_heartrate.synthetic_10_change_to_schema`
;

# Note: this step should narrow down to just the last ~60 minutes or so during pipeline process in production

CREATE OR REPLACE TABLE `hca-data-sandbox.fetal_heartrate.rules_classify_0_summarize_by_second5` AS
with pre_table as
(
  SELECT
      subjectid
    , cast((FORMAT_TIMESTAMP('%F %H:%M', a.measurement_timestamp )) || ':' || floor(extract(second from a.measurement_timestamp) / 5)*5 as timestamp) as measurement_timestamp
    , datatype
    , max(monitorid) as monitorid
    , max(sensortype) as sensortype
    , avg(case when sensortype in ('INOP','No_Trans') then 1 else 0 end) as data_quality
    , avg(value) as value
  -- SELECT *
  FROM `hca-data-sandbox.fetal_heartrate.rules_classify_0_union_all_input_data` a
  GROUP BY 1,2,3
)
SELECT
    subjectid
  , measurement_timestamp
  , datatype
  , monitorid
  , sensortype
  , case when data_quality = 0 then 'Valid' else 'Not Valid' end as data_quality
  , value
FROM pre_table
;

/**********************
I. Baseline: FHR, UA, US
**********************/

# Granularity
  -- By patient by every five seconds

# Method:
  -- Take last 30 min of data
  -- Remove incomplete data
  -- Remove top and bottom 25%
    # Note: this is a basic proxy for "excluding periods of marked FHR variability, periodic or episodic changes"
  -- Ensure at least 2 minutes of data
  -- Take the average value
  -- Then remove values that are more than 25 bpm off of average
  -- Then take new average

CREATE OR REPLACE TABLE `hca-data-sandbox.fetal_heartrate.rules_classify_1_baseline_last_30_min` AS
WITH array_summary_last_30_min as
  (
    # Order that array by values and remove invalid data
    SELECT *, ARRAY(SELECT x FROM UNNEST(array_values) AS x WHERE data_quality <> 'Invalid Data' ORDER BY x) as array_window
    FROM
    (
      SELECT
          *
        # Create an array of last 360 values (30 minutes * 60 sec/min / 5 second snippets)
        , ARRAY_AGG(value) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp ROWS BETWEEN 360 PRECEDING AND 0 FOLLOWING) as array_values
      FROM `hca-data-sandbox.fetal_heartrate.rules_classify_0_summarize_by_second5`
    ) a
  )
, identify_bottom_top_outliers as
(
SELECT
    *
    # Set bottom / top outliers
  , array_length(array_window) * 5 / 60 as count_minutes
  , array_window[SAFE_OFFSET(cast(floor((array_length(array_window) / 100) * 25) as int64))] as bottom_outliers
  , array_window[SAFE_OFFSET(cast(floor((array_length(array_window) / 100) * 75) as int64))] as top_outliers
FROM array_summary_last_30_min a
),
baseline_pre AS (
SELECT
    measurement_timestamp
  , subjectid
  , datatype
  , data_quality
  , value
  , array_window
  , count_minutes
  , bottom_outliers
  , top_outliers
# Take average of values between top and bottom otuliers, ensure at least 2 min of measurements
  , case
      when count_minutes > 2 then (SELECT avg(x) FROM UNNEST(array_window) x WHERE x >= bottom_outliers and x <= top_outliers)
      else NULL
    end as baseline_pre
FROM identify_bottom_top_outliers a
)
  SELECT
      measurement_timestamp
    , subjectid
    , datatype
    , data_quality
    , value
  # Remove values where average +/-25 from original baseline
    , case
        when count_minutes > 2 then (SELECT avg(x) FROM UNNEST(array_window) x WHERE x >= bottom_outliers and x <= top_outliers AND x BETWEEN baseline_pre - 25 and baseline_pre + 25)
        else NULL
      end as baseline
  FROM baseline_pre a
;

/**********************
II. Variability: FHR
**********************/

# Granularity
  -- By patient by quarter second

# Method:
  -- Take last 3 min of data
  -- Figure out # of cycles
     -- Take the difference between every point and the 5 seconds before it
     -- Every time number increases then decreases, that's a peak
     -- Every time number decreases then increaes, that's a nadir
     -- A full cycle is the # of times this changes / 2
  -- Figure out average amplitude of cycles
     -- Take difference between every peak and nadir / 2
  -- If cycle length average is > 30 seconds, that's less than 2 cycles / minute --> no variability
      -- Otherwise, take the average amplitude for the past 10 minutes as variability

CREATE OR REPLACE TABLE `hca-data-sandbox.fetal_heartrate.rules_classify_2_variability_last_3_min` AS
with prior_value AS
(
  SELECT
      # Calculate absolute difference between every point and quarter second
      *
    , LAST_VALUE(value) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS last_value
  FROM `hca-data-sandbox.fetal_heartrate.rules_classify_0_union_all_input_data` a
)
# For every value is it increasing or decreasing
, prior_value_change AS
(
  SELECT
      *
    , value - last_value as change
    , case
        when value - last_value < 0 then 'Decreasing'
        when value - last_value > 0 then 'Increasing'
        when value - last_value = 0 then 'No Change'
        else 'No Change'
      end as change_type
  FROM prior_value
)
# For every PRIOR value is it increasing or decreasing
, change_type_following as
(
  SELECT
      *
    , LAST_VALUE(change_type) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS change_type_after
  FROM prior_value_change
)
# If increasing then decreasing - peak; if decreasing then increasing - trough
, change_wave_type as (
  SELECT
      *
    , case
        when change_type = 'Decreasing' and change_type_after = 'Increasing' then 'Nadir'
        when change_type = 'Increasing' and change_type_after = 'Decreasing' then 'Peak'
        else NULL
      end as wave_type
  FROM change_type_following
)
# How long did it take for half cycle (peak to trough or trough to peak)? How long did it take for full cycle (peak to peak, trough to trough)? What was the value at each peak & trough?
, wave_summary_pre as (
  SELECT
      *
    , LAG(measurement_timestamp,1) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp) as last_half_cycle_time
    , LAG(measurement_timestamp,2) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp) as last_full_cycle_time
    , LAG(value,1) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp) as last_half_cycle_value
    , LAG(value,2) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp) as last_full_cycle_value
  FROM change_wave_type
  WHERE wave_type is not null
)
# Cycle length: length of full cycle; amplitude: difference in half cycle / 2
, wave_summary as
(
  SELECT
      measurement_timestamp
    , subjectid
    , datatype
    , value
    , timestamp_diff(measurement_timestamp, last_full_cycle_time, second) as cycle_length
    , abs((value - last_half_cycle_value)) / 2 as amplitude
  FROM wave_summary_pre
)
# For all points in between peaks and troughs, insert in the summary from the latest peak or trough
, fill_out_values_with_last_cycle as
(
  SELECT
      a.*
    , last_value(b.cycle_length ignore nulls) OVER (PARTITION BY a.subjectid, a.datatype ORDER BY a.measurement_timestamp ROWS BETWEEN 300 PRECEDING AND 0 PRECEDING) AS cycle_length
    , last_value(b.amplitude ignore nulls) OVER (PARTITION BY a.subjectid, a.datatype ORDER BY a.measurement_timestamp ROWS BETWEEN 300 PRECEDING AND 0 PRECEDING) AS amplitude
  FROM `hca-data-sandbox.fetal_heartrate.rules_classify_0_union_all_input_data` a
  LEFT JOIN wave_summary b
    ON a.measurement_timestamp = b.measurement_timestamp
    AND a.subjectid = b.subjectid
    AND a.datatype = b.datatype
)
# Then take average
, aggregate_to_every_5_seconds as
(
  SELECT
      subjectid
    , cast((FORMAT_TIMESTAMP('%F %H:%M', a.measurement_timestamp )) || ':' || floor(extract(second from a.measurement_timestamp) / 5)*5 as timestamp) as measurement_timestamp
    , datatype
    , avg(cycle_length) as cycle_length
    , avg(amplitude) as amplitude
  FROM fill_out_values_with_last_cycle a
  GROUP BY 1,2,3
)
# Then join it back
, join_back_to_main_table as
(
  SELECT
      a.*
    , b.cycle_length
    , b.amplitude
  FROM `hca-data-sandbox.fetal_heartrate.rules_classify_1_baseline_last_30_min` a
  LEFT JOIN aggregate_to_every_5_seconds b
    ON a.subjectid = b.subjectid
    AND a.measurement_timestamp = b.measurement_timestamp
    AND a.datatype = b.datatype
)
# Then take average of last rolling 3 minutes
, average_last_3_minutes as
(
  SELECT
      measurement_timestamp
    , subjectid
    , datatype
    , data_quality
    , value
    , baseline
    , avg(cycle_length) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp ROWS BETWEEN 36 PRECEDING AND 0 PRECEDING) as cycle_length
    , avg(amplitude) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp ROWS BETWEEN 36 PRECEDING AND 0 PRECEDING) as amplitude
  FROM join_back_to_main_table
)
# If cycle length > 30 seconds, it's null (docs say must have 2 cycles / minute); otherwise take average
SELECT
    measurement_timestamp
  , subjectid
  , datatype
  , data_quality
  , value
  , baseline
  , case when cycle_length > 30 then NULL else amplitude end as variability
FROM average_last_3_minutes
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
# for every point compare value to baseline
with is_event as (
  SELECT
  *
  , case when datatype in ('HR1','HR2') and value - baseline <= -15 then 1.0 else 0 end as is_change1 -- Dec
  , case when datatype in ('HR1','HR2') and value - baseline >= 15 then 1.0 else 0 end as is_change2 -- Acc
  , case when datatype in ('UA') and value - baseline >= 15 then 1.0 else 0 end as is_change3 -- Contract
  , case when datatype in ('US') and value - baseline >= 15 then 1.0 else 0 end as is_change4 -- Uterine Stim
  FROM `hca-data-sandbox.fetal_heartrate.rules_classify_2_variability_last_3_min`
),
# for every point compare last 3 values (15 seconds) - all 3 must be greater than 15
is_event_last_x_seconds as (
  SELECT
      *
    , AVG(is_change1) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp ROWS BETWEEN 2 PRECEDING AND 0 PRECEDING) AS last_x_seconds1 -- Dec
    , AVG(is_change2) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp ROWS BETWEEN 2 PRECEDING AND 0 PRECEDING) AS last_x_seconds2 -- Acc
    , AVG(is_change3) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp ROWS BETWEEN 2 PRECEDING AND 0 PRECEDING) AS last_x_seconds3 -- Contract
    , AVG(is_change4) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp ROWS BETWEEN 2 PRECEDING AND 0 PRECEDING) AS last_x_seconds4 -- Uterine Stim
  FROM is_event
),
# create "period state": if all 3 of prior points is not greater than 15, it could end an event; if all 3 of prior points is greater than 15, it could start an event
period_state_every_y_seconds as (
  SELECT
      *
    , case when last_x_seconds1 = 0 then 'End of Period' when last_x_seconds1 = 1 then 'Start of Period' else 'Unknown' end as period_state1 -- Dec
    , case when last_x_seconds2 = 0 then 'End of Period' when last_x_seconds2 = 1 then 'Start of Period' else 'Unknown' end as period_state2 -- Acc
    , case when last_x_seconds3 = 0 then 'End of Period' when last_x_seconds3 = 1 then 'Start of Period' else 'Unknown' end as period_state3 -- Contract
    , case when last_x_seconds4 = 0 then 'End of Period' when last_x_seconds4 = 1 then 'Start of Period' else 'Unknown' end as period_state4 -- Uterine Stim
  FROM is_event_last_x_seconds
),
# Take the period state for this records and the prior 5 seconds
period_state_every_y_seconds_and_prior as (
SELECT
    *
  , coalesce(last_value(period_state1) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING),'Unknown') AS period_state_prior1 -- Dec
  , coalesce(last_value(period_state2) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING),'Unknown') AS period_state_prior2 -- Acc
  , coalesce(last_value(period_state3) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING),'Unknown') AS period_state_prior3 -- Contract
  , coalesce(last_value(period_state4) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING),'Unknown') AS period_state_prior4 -- Uterine Stim
FROM period_state_every_y_seconds
) ,
# If Start of Period is involved in either, it's an event; if End of Period is involved in either, it's a not event
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
# For anything that is NULL, take the last value
period_state_every_y_seconds_and_prior_summary as (
SELECT
    *
  , last_value(is_event_pre1 IGNORE NULLS) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp ROWS BETWEEN 200 PRECEDING AND 0 PRECEDING) as is_event1 -- Dec
  , last_value(is_event_pre2 IGNORE NULLS) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp ROWS BETWEEN 200 PRECEDING AND 0 PRECEDING) as is_event2 -- Acc
  , last_value(is_event_pre3 IGNORE NULLS) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp ROWS BETWEEN 200 PRECEDING AND 0 PRECEDING) as is_event3 -- Contract
  , last_value(is_event_pre4 IGNORE NULLS) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp ROWS BETWEEN 200 PRECEDING AND 0 PRECEDING) as is_event4 -- Uterine Stim
FROM period_state_every_y_seconds_and_prior_summary_pre
),
# Now take the event state and the pior event state
period_state_every_y_seconds_and_prior_summary_prior as (
  SELECT
      *
    , last_value(is_event1 IGNORE NULLS) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) as is_event_prior1 -- Dec
    , last_value(is_event2 IGNORE NULLS) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) as is_event_prior2 -- Acc
    , last_value(is_event3 IGNORE NULLS) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) as is_event_prior3 -- Contract
    , last_value(is_event4 IGNORE NULLS) OVER (PARTITION BY subjectid, datatype ORDER BY measurement_timestamp ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) as is_event_prior4 -- Uterine Stim
  FROM period_state_every_y_seconds_and_prior_summary
),
# If you go from "Not Event" to "Event" that's the start of the event
# If you go from "Event" to "Not Event" that's the end of the event
event_start_end_date_pre as (
  SELECT
      *
    , case when is_event1 = 'Event' and is_event_prior1 = 'Not Event' then 'Event Start' when is_event1 = 'Not Event' and is_event_prior1 = 'Event' then 'Event End' end as event_start_end1 -- Dec
    , case when is_event2 = 'Event' and is_event_prior2 = 'Not Event' then 'Event Start' when is_event2 = 'Not Event' and is_event_prior2 = 'Event' then 'Event End' end as event_start_end2 -- Acc
    , case when is_event3 = 'Event' and is_event_prior3 = 'Not Event' then 'Event Start' when is_event3 = 'Not Event' and is_event_prior3 = 'Event' then 'Event End' end as event_start_end3 -- Contract
    , case when is_event4 = 'Event' and is_event_prior4 = 'Not Event' then 'Event Start' when is_event4 = 'Not Event' and is_event_prior4 = 'Event' then 'Event End' end as event_start_end4 -- Uterine Stim
  FROM period_state_every_y_seconds_and_prior_summary_prior
)
# Manually move back the event start by 15 seconds (I found that we start 15 seconds too late but end on time b/c we're waiting for 3 consecutive readings)
SELECT
    a.* except (event_start_end1, event_start_end2, event_start_end3, event_start_end4)
  , case when b.event_start_end1 = 'Event Start' then 'Event Start' when a.event_start_end1 = 'Event Start' then NULL else a.event_start_end1 end as event_start_end1
  , case when b.event_start_end2 = 'Event Start' then 'Event Start' when a.event_start_end2 = 'Event Start' then NULL else a.event_start_end2 end as event_start_end2
  , case when b.event_start_end3 = 'Event Start' then 'Event Start' when a.event_start_end3 = 'Event Start' then NULL else a.event_start_end3 end as event_start_end3
  , case when b.event_start_end4 = 'Event Start' then 'Event Start' when a.event_start_end4 = 'Event Start' then NULL else a.event_start_end4 end as event_start_end4
FROM event_start_end_date_pre a
LEFT JOIN event_start_end_date_pre b
  ON a.subjectid = b.subjectid
  AND a.datatype = b.datatype
  AND a.measurement_timestamp = timestamp_add(b.measurement_timestamp, interval -15 second)
;

CREATE OR REPLACE TABLE `hca-data-sandbox.fetal_heartrate.rules_classify_3_events_part2` AS
# Create an event ID for each distinct event
with period_state_every_y_seconds_event_id as (
  SELECT
      a.*
    , b.event_id as event_id1 -- Dec
    , c.event_id as event_id2 -- Acc
    , d.event_id as event_id3 -- Contract
    , e.event_id as event_id4 -- Uterine Stim
  FROM `hca-data-sandbox.fetal_heartrate.rules_classify_3_events_part1` a
  # Event ID is based on the rank of each event start and event end
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
# Then fill in all blank rows with the event ID if they occur between start and end
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
# Create a table that draws out the start and end points of all 4 event types + event IDs
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

-- Contraction
  -- start
  -- end
  -- length
  -- amplitude

CREATE OR REPLACE TABLE `hca-data-sandbox.fetal_heartrate.rules_classify_4_contraction` AS
# Break out each event
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
# Measure amplitude of event
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
),
# Measure how long it takes to reach peak
con_first_peak as
(
  SELECT
      b.subjectid
    , b.contraction_event_id
    , min(a.measurement_timestamp) as first_peak
  FROM `hca-data-sandbox.fetal_heartrate.rules_classify_2_variability_last_3_min` a
  CROSS JOIN contractions b
  CROSS JOIN con_amplitude c
  WHERE a.subjectid = b.subjectid
  AND a.measurement_timestamp >= b.contraction_start_time
  AND a.measurement_timestamp <= b.contraction_end_time
  AND a.subjectid = c.subjectid
  AND b.contraction_event_id = c.contraction_event_id
  AND abs(a.value - a.baseline) >= c.amplitude
  GROUP BY 1,2
)
  SELECT
      a.*
    , b.amplitude
    , c.first_peak
    , timestamp_diff(first_peak, contraction_start_time, second) as seconds_to_peak
  FROM contractions a
  LEFT JOIN con_amplitude b
    ON a.subjectid = b.subjectid
    AND a.contraction_event_id = b.contraction_event_id
  LEFT JOIN con_first_peak c
    ON a.subjectid = c.subjectid
    AND a.contraction_event_id = c.contraction_event_id
;

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
dec_first_peak as
(
  SELECT
      b.subjectid
    , b.deceleration_event_id
    , min(a.measurement_timestamp) as first_peak
  FROM `hca-data-sandbox.fetal_heartrate.rules_classify_2_variability_last_3_min` a
  CROSS JOIN decelerations b
  CROSS JOIN dec_amplitude c
  WHERE a.subjectid = b.subjectid
  AND a.measurement_timestamp >= b.deceleration_start_time
  AND a.measurement_timestamp <= b.deceleration_end_time
  AND a.subjectid = c.subjectid
  AND b.deceleration_event_id = c.deceleration_event_id
  AND abs(a.value - a.baseline) >= c.amplitude
  GROUP BY 1,2
),
# For decelerations and contractions, find the closest match
cross_join_dec_con as
(
  SELECT
      a.subjectid
    , a.deceleration_event_id
    , b.contraction_event_id
    , abs(timestamp_diff(a.deceleration_start_time, b.contraction_start_time, second)) as start_time_diff
    , abs(timestamp_diff(a.deceleration_end_time, b.contraction_end_time, second)) as end_time_diff
  FROM decelerations a
  LEFT JOIN `hca-data-sandbox.fetal_heartrate.rules_classify_4_contraction` b
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
# Write out stats on the match
dec_con_ordering as
(
  SELECT
      a.*
    , d.first_peak
    , timestamp_diff(d.first_peak, a.deceleration_start_time, second) as seconds_to_peak
    , c.contraction_start_time
    , c.contraction_end_time
    , c.contraction_length
    , timestamp_diff(a.deceleration_start_time, c.contraction_start_time, second) as start_time_diff
    , timestamp_diff(a.deceleration_end_time, c.contraction_end_time, second) as end_time_diff
    , c.first_peak as contraction_peak
    , timestamp_diff(d.first_peak, c.first_peak, second) as peak_time_diff
  FROM decelerations a
  LEFT JOIN dec_con_mapping b
    ON a.subjectid = b.subjectid
    AND a.deceleration_event_id = b.deceleration_event_id
  LEFT JOIN `hca-data-sandbox.fetal_heartrate.rules_classify_4_contraction` c
    ON a.subjectid = c.subjectid
    AND b.contraction_event_id = c.contraction_event_id
  LEFT JOIN dec_first_peak d
    ON a.subjectid = d.subjectid
    AND b.deceleration_event_id = d.deceleration_event_id
),
# Figure out how much overlap there was on match based on which started first and which ended first
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
    , a.first_peak
    , a.seconds_to_peak
    , a.contraction_start_time as matching_contraction_start
    , a.contraction_end_time as matching_contraction_end
    , a.contraction_length as matching_contraction_length
    , a.start_time_diff
    , a.end_time_diff
    , a.contraction_peak as matching_contraction_peak
    , a.peak_time_diff
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
),
acc_first_peak as
(
  SELECT
      b.subjectid
    , b.acceleration_event_id
    , min(a.measurement_timestamp) as first_peak
  FROM `hca-data-sandbox.fetal_heartrate.rules_classify_2_variability_last_3_min` a
  CROSS JOIN acceleration b
  CROSS JOIN acc_amplitude c
  WHERE a.subjectid = b.subjectid
  AND a.measurement_timestamp >= b.acceleration_start_time
  AND a.measurement_timestamp <= b.acceleration_end_time
  AND a.subjectid = c.subjectid
  AND b.acceleration_event_id = c.acceleration_event_id
  AND abs(a.value - a.baseline) >= c.amplitude
  GROUP BY 1,2
),
acc_prior as
(
  SELECT
      *
    , lag(acceleration_event_id) OVER (partition by subjectid ORDER BY acceleration_event_id) as prior_acc_event_id
    , lag(acceleration_start_time) OVER (partition by subjectid ORDER BY acceleration_event_id) as prior_acc_start_time
    , lag(acceleration_end_time) OVER (partition by subjectid ORDER BY acceleration_event_id) as prior_acc_end_time
  FROM acceleration
  ORDER BY 1,2
  LIMIT 10
)
SELECT
    a.*
  , b.amplitude
  , c.first_peak
  , timestamp_diff(first_peak, acceleration_start_time, second) as seconds_to_peak
FROM acc_prior a
LEFT JOIN acc_amplitude b
  ON a.subjectid = b.subjectid
  AND a.acceleration_event_id = b.acceleration_event_id
LEFT JOIN acc_first_peak c
  ON a.subjectid = c.subjectid
  AND a.acceleration_event_id = c.acceleration_event_id
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
    , b.amplitude
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

/**********************
V. Combine into Summary Table with Structs
**********************/

-- Pre table -- convert by subject by time by datatype to just by subject by time

CREATE OR REPLACE TABLE `hca-data-sandbox.fetal_heartrate.rules_classify_5_summary_table_pre` AS
# Pivot the table to have many more columns - instead of a PK of time, subject, and datatype, now the table PK is just time, subject
SELECT distinct
    a.measurement_timestamp
  , a.subjectid
  , b.value as fhr_value
  , b.baseline as fhr_baseline
  , b.variability as fhr_variability
  , b.data_quality as fhr_data_quality
  , c.value as ua_value
  , c.baseline as ua_baseline
  , c.variability as ua_variability
  , c.data_quality as ua_data_quality
  , d.value as us_value
  , d.baseline as us_baseline
  , d.variability as us_variability
  , d.data_quality as us_data_quality
FROM `hca-data-sandbox.fetal_heartrate.rules_classify_2_variability_last_3_min` a
LEFT JOIN
(
  SELECT *
  FROM `hca-data-sandbox.fetal_heartrate.rules_classify_2_variability_last_3_min`
  WHERE datatype in ('HR1','HR2')
) b
  ON a.measurement_timestamp = b.measurement_timestamp
  AND a.subjectid = b.subjectid
LEFT JOIN
(
  SELECT *
  FROM `hca-data-sandbox.fetal_heartrate.rules_classify_2_variability_last_3_min`
  WHERE datatype in ('UA')
) c
  ON a.measurement_timestamp = c.measurement_timestamp
  AND a.subjectid = c.subjectid
LEFT JOIN
(
  SELECT *
  FROM `hca-data-sandbox.fetal_heartrate.rules_classify_2_variability_last_3_min`
  WHERE datatype in ('US')
) d
  ON a.measurement_timestamp = d.measurement_timestamp
  AND a.subjectid = d.subjectid
;

-- PK: by subject, by every 5 seconds
-- Records for
  -- Baseline
  -- Variability
  -- Values for the last 10 minutes
  -- Events for the past hour
      -- Decelerations
      -- Contractions
      -- Accelerations
      -- Uterine Stimulation

CREATE OR REPLACE TABLE `hca-data-sandbox.fetal_heartrate.rules_classify_5_summary_table` AS
SELECT
    a.*
# Create an array that has the last hour of measurements (in case you want to measure % values at Bradycardia or Tachycardia
  , ARRAY_AGG(struct(a.measurement_timestamp, a.fhr_value)) OVER (PARTITION BY a.subjectid ORDER BY a.measurement_timestamp ROWS BETWEEN 720 PRECEDING AND 0 FOLLOWING) as measurements_last_60_min
# Create an array of structs with a row for every deceleration in the past hour
  , ARRAY_AGG(
      struct(
          b.deceleration_start_time
        , b.deceleration_end_time
        , b.deceleration_length
        , b.deceleration_amplitude
        , b.matching_contraction_start
        , b.deceleration_first_peak
        , b.deceleration_seconds_to_peak
        , b.matching_contraction_end
        , b.matching_contraction_length
        , b.matching_contraction_peak
        , b.start_time_diff
        , b.end_time_diff
        , b.peak_time_diff
        , b.type
        , b.seconds_overlap
      )
    ) as decelerations
# Same for contractions
  , ARRAY_AGG(
      struct(
          c.contraction_start_time
        , c.contraction_end_time
        , c.contraction_length
        , c.contraction_amplitude
        , c.contraction_first_peak
        , c.contraction_seconds_to_peak
      )
    ) as contractions
# Same for accelerations
  , ARRAY_AGG(
      struct(
          d.acceleration_start_time
        , d.acceleration_end_time
        , d.acceleration_length
        , d.acceleration_amplitude
        , d.prior_acc_event_id
        , d.prior_acc_start_time
        , d.prior_acc_end_time
        , d.acceleration_first_peak
        , d.acceleration_seconds_to_peak
      )
    ) as acceleration
# Same for uterine stimulation
  , ARRAY_AGG(
      struct(
          e.uterine_stimulation_start_time
        , e.uterine_stimulation_end_time
        , e.uterine_stimulation_length
        , e.uterine_stimulation_amplitude
        , e.matching_acceleration_start
        , e.matching_acceleration_end
        , e.matching_acceleration_length
        , e.start_time_diff
        , e.end_time_diff
        , e.type
        , e.seconds_overlap
      )
    ) as uterine_stimulation
FROM `hca-data-sandbox.fetal_heartrate.rules_classify_5_summary_table_pre` a
LEFT JOIN
(
  SELECT distinct
      a.measurement_timestamp
    , a.subjectid
    , b.deceleration_start_time
    , b.deceleration_end_time
    , b.deceleration_length
    , b.first_peak as deceleration_first_peak
    , b.seconds_to_peak as deceleration_seconds_to_peak
    , b.deceleration_amplitude
    , b.matching_contraction_start
    , b.matching_contraction_end
    , b.matching_contraction_length
    , b.matching_contraction_peak
    , b.start_time_diff
    , b.end_time_diff
    , b.peak_time_diff
    , b.type
    , b.seconds_overlap
  FROM `hca-data-sandbox.fetal_heartrate.rules_classify_5_summary_table_pre` a
  CROSS JOIN `hca-data-sandbox.fetal_heartrate.rules_classify_4_deceleration` b
  WHERE a.subjectid = b.subjectid
  AND a.measurement_timestamp BETWEEN b.deceleration_start_time AND timestamp_add(b.deceleration_start_time, interval 60 minute)
) b
  ON a.measurement_timestamp = b.measurement_timestamp
  AND a.subjectid = b.subjectid
LEFT JOIN
(
  SELECT distinct
      a.measurement_timestamp
    , a.subjectid
    , b.contraction_start_time
    , b.contraction_end_time
    , b.contraction_length
    , b.amplitude as contraction_amplitude
    , b.first_peak as contraction_first_peak
    , b.seconds_to_peak as contraction_seconds_to_peak
  FROM `hca-data-sandbox.fetal_heartrate.rules_classify_5_summary_table_pre` a
  CROSS JOIN `hca-data-sandbox.fetal_heartrate.rules_classify_4_contraction` b
  WHERE a.subjectid = b.subjectid
  AND a.measurement_timestamp BETWEEN b.contraction_start_time AND timestamp_add(b.contraction_start_time, interval 60 minute)
) c
  ON a.measurement_timestamp = c.measurement_timestamp
  AND a.subjectid = c.subjectid
LEFT JOIN
(
  SELECT distinct
      a.measurement_timestamp
    , a.subjectid
    , b.acceleration_start_time
    , b.acceleration_end_time
    , b.acceleration_length
    , b.amplitude as acceleration_amplitude
    , b.prior_acc_event_id
    , b.prior_acc_start_time
    , b.prior_acc_end_time
    , b.first_peak as acceleration_first_peak
    , b.seconds_to_peak as acceleration_seconds_to_peak
  FROM `hca-data-sandbox.fetal_heartrate.rules_classify_5_summary_table_pre` a
  CROSS JOIN `hca-data-sandbox.fetal_heartrate.rules_classify_4_acceleration` b
  WHERE a.subjectid = b.subjectid
  AND a.measurement_timestamp BETWEEN b.acceleration_start_time AND timestamp_add(b.acceleration_start_time, interval 60 minute)
) d
  ON a.measurement_timestamp = d.measurement_timestamp
  AND a.subjectid = d.subjectid
LEFT JOIN
(
  SELECT distinct
      a.measurement_timestamp
    , a.subjectid
    , b.uterine_stimulation_start_time
    , b.uterine_stimulation_end_time
    , b.uterine_stimulation_length
    , b.amplitude as uterine_stimulation_amplitude
    , b.matching_acceleration_start
    , b.matching_acceleration_end
    , b.matching_acceleration_length
    , b.start_time_diff
    , b.end_time_diff
    , b.type
    , b.seconds_overlap
  FROM `hca-data-sandbox.fetal_heartrate.rules_classify_5_summary_table_pre` a
  CROSS JOIN `hca-data-sandbox.fetal_heartrate.rules_classify_4_uterine_stimulation` b
  WHERE a.subjectid = b.subjectid
  AND a.measurement_timestamp BETWEEN b.uterine_stimulation_start_time AND timestamp_add(b.uterine_stimulation_start_time, interval 60 minute)
) e
  ON a.measurement_timestamp = e.measurement_timestamp
  AND a.subjectid = e.subjectid
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
;
```
