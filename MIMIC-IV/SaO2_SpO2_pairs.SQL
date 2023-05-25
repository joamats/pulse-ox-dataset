-- Limiting variable: SaO2
-- We fetch all possible SaO2. Each of these is aligned with the closest SpO2 value, in a 5min window, and record the delta

DROP TABLE IF EXISTS `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs`;
CREATE TABLE `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs` AS

-- Auxiliary to fetch the SaO2 timestamps and get the corresponding SpO2
WITH pairs AS (
  SELECT
    sao2_table.subject_id
  , sao2_table.stay_id
  , sao2_table.charttime AS SaO2_timestamp
  , MAX(spo2_table.charttime) AS SpO2_timestamp

  FROM `physionet-data.mimiciv_icu.chartevents` AS sao2_table

  LEFT JOIN(
    SELECT 
      stay_id
    , charttime
    , CASE WHEN itemid IN (220277) AND valuenum > 0 AND valuenum <= 100 THEN valuenum ELSE NULL END AS SpO2
    FROM `physionet-data.mimiciv_icu.chartevents` spo2_table
    WHERE itemid = 220277 -- SpO2
  )
  AS spo2_table
  ON spo2_table.stay_id = sao2_table.stay_id
  -- each ABG-measured sao2_table is matched with the closest SpO2 value recorded within the previous 5 minutes"
  AND TIMESTAMP_DIFF(sao2_table.charttime, spo2_table.charttime, MINUTE) <= 5
  AND TIMESTAMP_DIFF(sao2_table.charttime, spo2_table.charttime, MINUTE) >= 0

  -- Only for SaO2 values
  WHERE sao2_table.itemid = 220227 -- SaO2
  GROUP BY subject_id, stay_id, SaO2_timestamp
  ORDER BY SaO2_timestamp ASC
)

SELECT
    pairs.subject_id
  , pairs.stay_id
  , SaO2_timestamp
  , sao2_vals.valuenum AS SaO2
  , TIMESTAMP_DIFF(SpO2_timestamp, SaO2_timestamp, MINUTE) AS delta_SpO2
  , spo2_vals.valuenum AS SpO2

FROM pairs

LEFT JOIN `physionet-data.mimiciv_icu.chartevents`
AS sao2_vals
ON sao2_vals.subject_id = pairs.subject_id
AND sao2_vals.stay_id = pairs.stay_id
AND sao2_vals.charttime = pairs.SaO2_timestamp
AND sao2_vals.itemid = 220227

LEFT JOIN `physionet-data.mimiciv_icu.chartevents`
AS spo2_vals
ON spo2_vals.subject_id = pairs.subject_id
AND spo2_vals.stay_id = pairs.stay_id
AND spo2_vals.charttime = pairs.SpO2_timestamp
AND spo2_vals.itemid = 220277

WHERE spo2_vals.valuenum IS NOT NULL
  AND sao2_vals.valuenum IS NOT NULL

ORDER BY subject_id, stay_id, SaO2_timestamp ASC