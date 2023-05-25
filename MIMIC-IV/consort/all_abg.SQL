-- Auxiliary to fetch the SaO2 timestamps and get the corresponding SpO2
WITH pairs AS (
  SELECT
    sao2_table.subject_id
  , sao2_table.stay_id
  , sao2_table.charttime AS SaO2_timestamp

  FROM `physionet-data.mimiciv_icu.chartevents` AS sao2_table

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
  , pat.sex_female
  , pat.anchor_age
  , pat.race_group
  , pat.language
  , pat.insurance

FROM pairs

LEFT JOIN `physionet-data.mimiciv_icu.chartevents`
AS sao2_vals
ON sao2_vals.subject_id = pairs.subject_id
AND sao2_vals.stay_id = pairs.stay_id
AND sao2_vals.charttime = pairs.SaO2_timestamp
AND sao2_vals.itemid = 220227

LEFT JOIN `protean-chassis-368116.mimiciv_pulseOx.patient_ICU`
AS pat
ON pat.subject_id = pairs.subject_id

WHERE sao2_vals.valuenum IS NOT NULL

ORDER BY subject_id, stay_id, SaO2_timestamp ASC