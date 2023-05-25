-- Coagualtion Values include d_dimer, fibrinogen, thrombin, inr (international normalized ratio), pt (prothrombin time), ptt (partial thromboplastin time)

DROP TABLE IF EXISTS `db_name.mimiciv_pulseOx.coagulation`;
CREATE TABLE `db_name.mimiciv_pulseOx.coagulation` AS

WITH 

  d_dimer AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , d_dimer
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(coag.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_d_dimer
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, coag.charttime, MINUTE)) ASC) AS seq

    FROM `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.coagulation` 
    AS coag
    ON coag.subject_id = pairs.subject_id
    AND d_dimer IS NOT NULL
    AND TIMESTAMP_DIFF(pairs.SaO2_timestamp, coag.charttime,MINUTE) >= 0

  ) 
  WHERE seq = 1

)

, fibrinogen AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , fibrinogen
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(coag.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_fibrinogen
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, coag.charttime, MINUTE)) ASC) AS seq

    FROM `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.coagulation` 
    AS coag
    ON coag.subject_id = pairs.subject_id
    AND fibrinogen IS NOT NULL
    AND TIMESTAMP_DIFF(pairs.SaO2_timestamp, coag.charttime,MINUTE) >= 0

  ) 
  WHERE seq = 1

)

, thrombin AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , thrombin
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(coag.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_thrombin
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, coag.charttime, MINUTE)) ASC) AS seq

    FROM `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.coagulation` 
    AS coag
    ON coag.subject_id = pairs.subject_id
    AND thrombin IS NOT NULL
    AND TIMESTAMP_DIFF(pairs.SaO2_timestamp, coag.charttime,MINUTE) >= 0

  ) 
  WHERE seq = 1

)

, inr AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , inr
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(coag.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_inr
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, coag.charttime, MINUTE)) ASC) AS seq

    FROM `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.coagulation` 
    AS coag
    ON coag.subject_id = pairs.subject_id
    AND inr IS NOT NULL
    AND TIMESTAMP_DIFF(pairs.SaO2_timestamp, coag.charttime,MINUTE) >= 0

  ) 
  WHERE seq = 1

)

, pt AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , pt
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(coag.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_pt
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, coag.charttime, MINUTE)) ASC) AS seq

    FROM `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.coagulation` 
    AS coag
    ON coag.subject_id = pairs.subject_id
    AND pt IS NOT NULL
    AND TIMESTAMP_DIFF(pairs.SaO2_timestamp, coag.charttime,MINUTE) >= 0

  ) 
  WHERE seq = 1

)

, ptt AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , ptt
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(coag.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_ptt
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, coag.charttime, MINUTE)) ASC) AS seq

    FROM `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.coagulation` 
    AS coag
    ON coag.subject_id = pairs.subject_id
    AND ptt IS NOT NULL
    AND TIMESTAMP_DIFF(pairs.SaO2_timestamp, coag.charttime,MINUTE) >= 0

  ) 
  WHERE seq = 1

)

SELECT 
    pairs.subject_id
  , pairs.stay_id
  , pairs.SaO2_timestamp
  , d_dimer.delta_d_dimer
  , d_dimer.d_dimer
  , fibrinogen.delta_fibrinogen
  , fibrinogen.fibrinogen
  , thrombin.delta_thrombin
  , thrombin.thrombin
  , inr.delta_inr
  , inr.inr
  , pt.delta_pt
  , pt.pt
  , ptt.delta_ptt
  , ptt.ptt

FROM `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

LEFT JOIN d_dimer
ON d_dimer.subject_id = pairs.subject_id
AND d_dimer.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN fibrinogen
ON fibrinogen.subject_id = pairs.subject_id
AND fibrinogen.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN thrombin
ON thrombin.subject_id = pairs.subject_id
AND thrombin.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN inr
ON inr.subject_id = pairs.subject_id
AND inr.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN pt
ON pt.subject_id = pairs.subject_id
AND pt.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN ptt
ON ptt.subject_id = pairs.subject_id
AND ptt.SaO2_timestamp = pairs.SaO2_timestamp

ORDER BY subject_id, stay_id, SaO2_timestamp