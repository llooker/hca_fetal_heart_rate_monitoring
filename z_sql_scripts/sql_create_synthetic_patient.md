```
/**********************
Purpose: Create fake data for F4 use case
Author: Aaron Wilkowitz
Date Created: 2021-10-19
**********************/

/**********************
Create baseline:
  - 3 hours of 140 bpm
  - Measurement every 1/4 second (43,200 rows total)
  - 25 bpm variability
**********************/

-- Generate rows

CREATE OR REPLACE TABLE `hca-data-sandbox.fetal_heartrate.synthetic_1_create_rows` AS
          SELECT 1 as value
UNION ALL SELECT 2 as value
UNION ALL SELECT 3 as value
UNION ALL SELECT 4 as value
UNION ALL SELECT 5 as value
UNION ALL SELECT 6 as value
UNION ALL SELECT 7 as value
UNION ALL SELECT 8 as value
UNION ALL SELECT 9 as value
UNION ALL SELECT 10 as value
;

-- Add in baseline values

CREATE OR REPLACE TABLE `hca-data-sandbox.fetal_heartrate.synthetic_2_add_in_baseline` AS
SELECT
    'Fake Patient A' as patient_name
  , cast('2021-06-15 9:00:00.00 UTC' as timestamp) as measurement_timestamp
  , 140 as fetal_heart_rate
  , 20 as uterine_pressure
  , 0 as uterine_stimulation
  , row_number() over (partition by 'x') as row_num
FROM `hca-data-sandbox.fetal_heartrate.synthetic_1_create_rows` a -- 10
CROSS JOIN `hca-data-sandbox.fetal_heartrate.synthetic_1_create_rows` b -- 100
CROSS JOIN `hca-data-sandbox.fetal_heartrate.synthetic_1_create_rows` c -- 1,000
CROSS JOIN `hca-data-sandbox.fetal_heartrate.synthetic_1_create_rows` d -- 10,000
CROSS JOIN `hca-data-sandbox.fetal_heartrate.synthetic_1_create_rows` e -- 100,000
;

-- Add in time

CREATE OR REPLACE TABLE `hca-data-sandbox.fetal_heartrate.synthetic_3_add_in_time` AS
SELECT
    patient_name
  , timestamp_add(measurement_timestamp, interval row_num*250 MILLISECOND) as measurement_timestamp
  , fetal_heart_rate
  , uterine_pressure
  , uterine_stimulation
  , row_num
FROM `hca-data-sandbox.fetal_heartrate.synthetic_2_add_in_baseline`
WHERE row_num <= 43200
;

-- Add in variability - 25 bpm

CREATE OR REPLACE TABLE `hca-data-sandbox.fetal_heartrate.synthetic_4_add_in_variability` AS
SELECT
    patient_name
  , measurement_timestamp
  , case
      when measurement_timestamp < '2021-06-15 09:36:00 UTC' then fetal_heart_rate + ((rand() - 0.5) * 50)
      when measurement_timestamp < '2021-06-15 10:36:00 UTC' then fetal_heart_rate + ((rand() - 0.5) * 15)
      when measurement_timestamp < '2021-06-15 11:36:00 UTC' then fetal_heart_rate + ((rand() - 0.5) * 5)
      else fetal_heart_rate + ((rand() - 0.5) * 2)
    end as fetal_heart_rate
  , uterine_pressure
  , uterine_stimulation
  , row_num
FROM `hca-data-sandbox.fetal_heartrate.synthetic_3_add_in_time`
ORDER BY 2
;

-- Every 36 minutes (18 min in), add in acceleration for 2 minutes

CREATE OR REPLACE TABLE `hca-data-sandbox.fetal_heartrate.synthetic_5_add_in_accelerations_distinct_mintues` AS
SELECT row_num, min(distinct_minute) as minute
FROM
(
  SELECT
    distinct_minute,
    floor(row_num / 36) as row_num
  FROM
  (
    SELECT
          FORMAT_TIMESTAMP('%F %H:%M', timestamp_add(measurement_timestamp, interval 18 minute) ) as distinct_minute
        , row_number() over (partition by 'x') as row_num
    FROM `hca-data-sandbox.fetal_heartrate.synthetic_4_add_in_variability`
    GROUP BY 1
    ORDER BY 1
  ) a
) a
WHERE row_num not in (3,4,5)
  --remove 4th acceleration b/c no longer responding to stimulation
  --remove 5th acceleration b/c after time
GROUP BY 1
ORDER BY 1
;

CREATE OR REPLACE TABLE `hca-data-sandbox.fetal_heartrate.synthetic_5_add_in_accelerations` AS
SELECT
    a.patient_name
  , a.measurement_timestamp
  , case when b.measurement_timestamp is not null then a.fetal_heart_rate + 30 else a.fetal_heart_rate END as fetal_heart_rate
  , a.uterine_pressure
  , a.uterine_stimulation
  , a.row_num
FROM `hca-data-sandbox.fetal_heartrate.synthetic_4_add_in_variability` a
LEFT JOIN
(
  SELECT a.measurement_timestamp
  FROM (SELECT 1 as forcefk, * FROM `hca-data-sandbox.fetal_heartrate.synthetic_4_add_in_variability`) a
  CROSS JOIN (SELECT 1 as forcefk, * FROM `hca-data-sandbox.fetal_heartrate.synthetic_5_add_in_accelerations_distinct_mintues`) b
  WHERE a.forcefk = b.forcefk
    AND FORMAT_TIMESTAMP('%F %H:%M', a.measurement_timestamp ) < b.minute
    AND FORMAT_TIMESTAMP('%F %H:%M', timestamp_add(a.measurement_timestamp, interval 3 minute)) > b.minute
) b
  ON a.measurement_timestamp = b.measurement_timestamp
ORDER BY 2
;

--   SELECT FORMAT_TIMESTAMP('%F %H:%M', a.measurement_timestamp ), count(*)
--   FROM (SELECT 1 as forcefk, * FROM `hca-data-sandbox.fetal_heartrate.synthetic_4_add_in_variability`) a
--   CROSS JOIN (SELECT 1 as forcefk, * FROM `hca-data-sandbox.fetal_heartrate.synthetic_5_add_in_accelerations_distinct_mintues`) b
--   WHERE a.forcefk = b.forcefk
--     AND FORMAT_TIMESTAMP('%F %H:%M', a.measurement_timestamp ) < b.minute
--     AND FORMAT_TIMESTAMP('%F %H:%M', timestamp_add(a.measurement_timestamp, interval 3 minute)) > b.minute
--   GROUP BY 1
--   ORDER BY 1

-- Every 18 minutes (9 min in), add in contraction for 6 minutes

CREATE OR REPLACE TABLE `hca-data-sandbox.fetal_heartrate.synthetic_6_add_in_contractions_distinct_mintues` AS
with contractions_by_row as (
  SELECT row_num, min(distinct_minute) as minute
  FROM
  (
    SELECT
      distinct_minute,
      floor(row_num / 18) as row_num
    FROM
    (
      SELECT
            FORMAT_TIMESTAMP('%F %H:%M', timestamp_add(measurement_timestamp, interval 9 minute) ) as distinct_minute
          , row_number() over (partition by 'x') as row_num
      FROM `hca-data-sandbox.fetal_heartrate.synthetic_5_add_in_accelerations`
      GROUP BY 1
      ORDER BY 1
    ) a
  ) a
  WHERE row_num not in (10)
    --remove 10th contraction b/c after time
  GROUP BY 1
  ORDER BY 1
)
SELECT
  row_num,
  -- for 1st contraction, make it several minutes later
  case when row_num = 0 then FORMAT_TIMESTAMP('%F %H:%M',timestamp_add(cast(minute || ':00.00 UTC' as timestamp), interval 5 minute)) else minute end as minute
FROM contractions_by_row
;

CREATE OR REPLACE TABLE `hca-data-sandbox.fetal_heartrate.synthetic_6_add_in_contractions` AS
SELECT
    a.patient_name
  , a.measurement_timestamp
  , a.fetal_heart_rate
  , case when b.measurement_timestamp is not null then a.uterine_pressure + (rand() * 50) else a.uterine_pressure END as uterine_pressure
  , a.uterine_stimulation
  , a.row_num
FROM `hca-data-sandbox.fetal_heartrate.synthetic_5_add_in_accelerations` a
LEFT JOIN
(
  SELECT a.measurement_timestamp
  FROM (SELECT 1 as forcefk, * FROM `hca-data-sandbox.fetal_heartrate.synthetic_5_add_in_accelerations`) a
  CROSS JOIN (SELECT 1 as forcefk, * FROM `hca-data-sandbox.fetal_heartrate.synthetic_6_add_in_contractions_distinct_mintues`) b
  WHERE a.forcefk = b.forcefk
    AND FORMAT_TIMESTAMP('%F %H:%M', a.measurement_timestamp ) < b.minute
    AND FORMAT_TIMESTAMP('%F %H:%M', timestamp_add(a.measurement_timestamp, interval 6 minute)) > b.minute
) b
  ON a.measurement_timestamp = b.measurement_timestamp
ORDER BY 2
;

-- Decelerations -- 6 decelerations, each lasting 6 minutes
  -- 1st and 2nd: early, right on time with contraction
  -- 3rd, 4th: 1 minute before
  -- 5th, 6th, 7th, 8th, 9th, 10th: 3 minutes after (right at apex)

CREATE OR REPLACE TABLE `hca-data-sandbox.fetal_heartrate.synthetic_7_add_in_decelerations_distinct_mintues` AS
SELECT
  case
    when row_num in (0,1) then minute
    when row_num in (2,3) then FORMAT_TIMESTAMP('%F %H:%M',timestamp_add(cast(minute || ':00.00 UTC' as timestamp), interval -1 minute))
    when row_num in (4,5,6,7,8,9) then FORMAT_TIMESTAMP('%F %H:%M',timestamp_add(cast(minute || ':00.00 UTC' as timestamp), interval 3 minute))
  end as minute
FROM `hca-data-sandbox.fetal_heartrate.synthetic_6_add_in_contractions_distinct_mintues`
;

CREATE OR REPLACE TABLE `hca-data-sandbox.fetal_heartrate.synthetic_7_add_in_decelerations` AS
SELECT
    a.patient_name
  , a.measurement_timestamp
  , case when b.measurement_timestamp is not null then a.fetal_heart_rate - 30 else a.fetal_heart_rate END as fetal_heart_rate
  , a.uterine_pressure
  , a.uterine_stimulation
  , a.row_num
FROM `hca-data-sandbox.fetal_heartrate.synthetic_6_add_in_contractions` a
LEFT JOIN
(
  SELECT a.measurement_timestamp
  FROM (SELECT 1 as forcefk, * FROM `hca-data-sandbox.fetal_heartrate.synthetic_6_add_in_contractions`) a
  CROSS JOIN (SELECT 1 as forcefk, * FROM `hca-data-sandbox.fetal_heartrate.synthetic_7_add_in_decelerations_distinct_mintues`) b
  WHERE a.forcefk = b.forcefk
    AND FORMAT_TIMESTAMP('%F %H:%M', a.measurement_timestamp ) < b.minute
    AND FORMAT_TIMESTAMP('%F %H:%M', timestamp_add(a.measurement_timestamp, interval 6 minute)) > b.minute
) b
  ON a.measurement_timestamp = b.measurement_timestamp
ORDER BY 2
;

--   SELECT FORMAT_TIMESTAMP('%F %H:%M', a.measurement_timestamp ), count(*)
--   FROM (SELECT 1 as forcefk, * FROM `hca-data-sandbox.fetal_heartrate.synthetic_6_add_in_contractions`) a
--   CROSS JOIN (SELECT 1 as forcefk, * FROM `hca-data-sandbox.fetal_heartrate.synthetic_7_add_in_decelerations_distinct_mintues`) b
--   WHERE a.forcefk = b.forcefk
--     AND FORMAT_TIMESTAMP('%F %H:%M', a.measurement_timestamp ) < b.minute
--     AND FORMAT_TIMESTAMP('%F %H:%M', timestamp_add(a.measurement_timestamp, interval 6 minute)) > b.minute
--   GROUP BY 1
--   ORDER BY 1

-- Add in uterine stimulation - this should be 36 minutes after the last acceleration, lasting for 2 minutes

CREATE OR REPLACE TABLE `hca-data-sandbox.fetal_heartrate.synthetic_8_add_in_uterine_stimulation_distinct_mintues` AS
SELECT FORMAT_TIMESTAMP('%F %H:%M',timestamp_add(cast(minute || ':00.00 UTC' as timestamp), interval 36 minute)) as minute
FROM `hca-data-sandbox.fetal_heartrate.synthetic_5_add_in_accelerations_distinct_mintues`
WHERE row_num = 2
UNION ALL
SELECT FORMAT_TIMESTAMP('%F %H:%M',timestamp_add(cast(minute || ':00.00 UTC' as timestamp), interval 72 minute)) as minute
FROM `hca-data-sandbox.fetal_heartrate.synthetic_5_add_in_accelerations_distinct_mintues`
WHERE row_num = 2
;

CREATE OR REPLACE TABLE `hca-data-sandbox.fetal_heartrate.synthetic_8_add_in_uterine_stimulation` AS
SELECT
    a.patient_name
  , a.measurement_timestamp
  , a. fetal_heart_rate
  , a.uterine_pressure
  , case when b.measurement_timestamp is not null then a.uterine_stimulation + 30 else a.uterine_stimulation END as uterine_stimulation
  , a.row_num
FROM `hca-data-sandbox.fetal_heartrate.synthetic_7_add_in_decelerations` a
LEFT JOIN
(
  SELECT a.measurement_timestamp
  FROM (SELECT 1 as forcefk, * FROM `hca-data-sandbox.fetal_heartrate.synthetic_7_add_in_decelerations`) a
  CROSS JOIN (SELECT 1 as forcefk, * FROM `hca-data-sandbox.fetal_heartrate.synthetic_8_add_in_uterine_stimulation_distinct_mintues`) b
  WHERE a.forcefk = b.forcefk
    AND FORMAT_TIMESTAMP('%F %H:%M', a.measurement_timestamp ) < b.minute
    AND FORMAT_TIMESTAMP('%F %H:%M', timestamp_add(a.measurement_timestamp, interval 2 minute)) > b.minute
) b
  ON a.measurement_timestamp = b.measurement_timestamp
ORDER BY 2
;

-- Add in flatlining during non-acceleration, non-decelartion during the final 30 minutes

CREATE OR REPLACE TABLE `hca-data-sandbox.fetal_heartrate.synthetic_9_add_in_flatlining_distinct_mintues` AS
SELECT FORMAT_TIMESTAMP('%F %H:%M', measurement_timestamp) as minute
FROM `hca-data-sandbox.fetal_heartrate.synthetic_8_add_in_uterine_stimulation`
WHERE measurement_timestamp > '2021-06-15 11:30:00.00 UTC'
GROUP BY 1
HAVING avg(fetal_heart_rate) BETWEEN 130 AND 150
;

CREATE OR REPLACE TABLE `hca-data-sandbox.fetal_heartrate.synthetic_9_add_in_flatlining` AS
SELECT
    a.patient_name
  , a.measurement_timestamp
  , case when b.measurement_timestamp is not null then 140 else a.fetal_heart_rate end as fetal_heart_rate
  , a.uterine_pressure
  , a.uterine_stimulation
  , a.row_num
FROM `hca-data-sandbox.fetal_heartrate.synthetic_8_add_in_uterine_stimulation` a
LEFT JOIN
(
  SELECT a.measurement_timestamp
  FROM (SELECT 1 as forcefk, * FROM `hca-data-sandbox.fetal_heartrate.synthetic_8_add_in_uterine_stimulation`) a
  CROSS JOIN (SELECT 1 as forcefk, * FROM `hca-data-sandbox.fetal_heartrate.synthetic_9_add_in_flatlining_distinct_mintues`) b
  WHERE a.forcefk = b.forcefk
    AND FORMAT_TIMESTAMP('%F %H:%M', a.measurement_timestamp ) = b.minute
) b
  ON a.measurement_timestamp = b.measurement_timestamp
ORDER BY 2
;

-- Transform to expected schema

  # synthetic_4_add_in_variability
  # synthetic_5_add_in_accelerations
  # synthetic_6_add_in_contractions
  # synthetic_7_add_in_decelerations
  # synthetic_8_add_in_uterine_stimulation
  # synthetic_9_add_in_flatlining

CREATE OR REPLACE TABLE `hca-data-sandbox.fetal_heartrate.synthetic_10_change_to_schema` AS
  SELECT
      patient_name as subjectid
    , measurement_timestamp as measurement_timestamp
    , 'HR2' as datatype
    , 'HP 135x' as monitorID
    , 'external' as sensortype
    , fetal_heart_rate as value
  FROM `hca-data-sandbox.fetal_heartrate.synthetic_9_add_in_flatlining`
UNION ALL
  SELECT
      patient_name as subjectid
    , measurement_timestamp as measurement_timestamp
    , 'UA' as datatype
    , 'HP 135x' as monitorID
    , 'TOCO' as sensortype
    , uterine_pressure as value
  FROM `hca-data-sandbox.fetal_heartrate.synthetic_9_add_in_flatlining`
UNION ALL
  SELECT
      patient_name as subjectid
    , measurement_timestamp as measurement_timestamp
    , 'US' as datatype
    , 'HP 135x' as monitorID
    , 'IUP' as sensortype
    , uterine_stimulation as value
  FROM `hca-data-sandbox.fetal_heartrate.synthetic_9_add_in_flatlining`
```
