-- Blood Values include hemoglobin, hematocrit, mch, mchc, mcv, platelet, rbc, rdw, wbc

DROP TABLE IF EXISTS `db_name.mimiciv_pulseOx.blood_count`;
CREATE TABLE `db_name.mimiciv_pulseOx.blood_count` AS

WITH 
  hemoglobin AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , hemoglobin
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(blood_counts.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_hemoglobin
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, blood_counts.charttime, MINUTE)) ASC) AS seq

    FROM `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.complete_blood_count` 
    AS blood_counts
    ON blood_counts.subject_id = pairs.subject_id
    AND hemoglobin IS NOT NULL
    AND TIMESTAMP_DIFF(pairs.SaO2_timestamp, blood_counts.charttime,MINUTE) >= 0

  ) 
  WHERE seq = 1

)

, hematocrit AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , hematocrit
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(blood_counts.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_hematocrit
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, blood_counts.charttime, MINUTE)) ASC) AS seq

    FROM `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.complete_blood_count` 
    AS blood_counts
    ON blood_counts.subject_id = pairs.subject_id
    AND hematocrit IS NOT NULL
    AND TIMESTAMP_DIFF(pairs.SaO2_timestamp, blood_counts.charttime,MINUTE) >= 0

  ) 
  WHERE seq = 1

)

, mch AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , mch
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(blood_counts.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_mch
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, blood_counts.charttime, MINUTE)) ASC) AS seq

    FROM `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.complete_blood_count` 
    AS blood_counts
    ON blood_counts.subject_id = pairs.subject_id
    AND mch IS NOT NULL
    AND TIMESTAMP_DIFF(pairs.SaO2_timestamp, blood_counts.charttime,MINUTE) >= 0


  ) 
  WHERE seq = 1

)

, mchc AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , mchc
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(blood_counts.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_mchc
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, blood_counts.charttime, MINUTE)) ASC) AS seq

    FROM `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.complete_blood_count` 
    AS blood_counts
    ON blood_counts.subject_id = pairs.subject_id
    AND mchc IS NOT NULL
    AND TIMESTAMP_DIFF(pairs.SaO2_timestamp, blood_counts.charttime,MINUTE) >= 0


  ) 
  WHERE seq = 1

)

, mcv AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , mcv
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(blood_counts.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_mcv
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, blood_counts.charttime, MINUTE)) ASC) AS seq

    FROM `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.complete_blood_count` 
    AS blood_counts
    ON blood_counts.subject_id = pairs.subject_id
    AND mcv IS NOT NULL
    AND TIMESTAMP_DIFF(pairs.SaO2_timestamp, blood_counts.charttime,MINUTE) >= 0


  ) 
  WHERE seq = 1

)

, platelet AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , platelet
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(blood_counts.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_platelet
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, blood_counts.charttime, MINUTE)) ASC) AS seq

    FROM `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.complete_blood_count` 
    AS blood_counts
    ON blood_counts.subject_id = pairs.subject_id
    AND platelet IS NOT NULL
    AND TIMESTAMP_DIFF(pairs.SaO2_timestamp, blood_counts.charttime,MINUTE) >= 0


  ) 
  WHERE seq = 1

)

, rbc AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , rbc
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(blood_counts.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_rbc
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, blood_counts.charttime, MINUTE)) ASC) AS seq

    FROM `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.complete_blood_count` 
    AS blood_counts
    ON blood_counts.subject_id = pairs.subject_id
    AND rbc IS NOT NULL
    AND TIMESTAMP_DIFF(pairs.SaO2_timestamp, blood_counts.charttime,MINUTE) >= 0


  ) 
  WHERE seq = 1

)

, rdw AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , rdw
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(blood_counts.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_rdw
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, blood_counts.charttime, MINUTE)) ASC) AS seq

    FROM `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.complete_blood_count` 
    AS blood_counts
    ON blood_counts.subject_id = pairs.subject_id
    AND rdw IS NOT NULL
    AND TIMESTAMP_DIFF(pairs.SaO2_timestamp, blood_counts.charttime,MINUTE) >= 0


  ) 
  WHERE seq = 1
  
)

, wbc AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , wbc
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(blood_counts.charttime,pairs.SaO2_timestamp, MINUTE) AS delta_wbc
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, blood_counts.charttime, MINUTE)) ASC) AS seq

    FROM `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.complete_blood_count` 
    AS blood_counts
    ON blood_counts.subject_id = pairs.subject_id
    AND wbc IS NOT NULL
    AND TIMESTAMP_DIFF(pairs.SaO2_timestamp, blood_counts.charttime,MINUTE) >= 0


  ) 
  WHERE seq = 1

)

SELECT 
    pairs.subject_id
  , pairs.stay_id
  , pairs.SaO2_timestamp
  , hemoglobin.delta_hemoglobin
  , hemoglobin.hemoglobin
  , hematocrit.delta_hematocrit
  , hematocrit.hematocrit
  , mch.delta_mch
  , mch.mch
  , mchc.delta_mchc
  , mchc.mchc
  , mcv.delta_mcv
  , mcv.mcv
  , platelet.delta_platelet
  , platelet.platelet
  , rbc.delta_rbc
  , rbc.rbc
  , rdw.delta_rdw
  , rdw.rdw
  , wbc.delta_wbc
  , wbc.wbc

FROM `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

LEFT JOIN hemoglobin
ON hemoglobin.subject_id = pairs.subject_id
AND hemoglobin.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN hematocrit
ON hematocrit.subject_id = pairs.subject_id
AND hematocrit.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN mch
ON mch.subject_id = pairs.subject_id
AND mch.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN mchc
ON mchc.subject_id = pairs.subject_id
AND mchc.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN mcv
ON mcv.subject_id = pairs.subject_id
AND mcv.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN platelet
ON platelet.subject_id = pairs.subject_id
AND platelet.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN rbc
ON rbc.subject_id = pairs.subject_id
AND rbc.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN rdw
ON rdw.subject_id = pairs.subject_id
AND rdw.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN wbc
ON wbc.subject_id = pairs.subject_id
AND wbc.SaO2_timestamp = pairs.SaO2_timestamp

ORDER BY subject_id, stay_id, SaO2_timestamp