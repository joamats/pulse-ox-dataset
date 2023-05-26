-- Chemistry Values include albumin, aniongap, bicarbonate, bun, calcium, chloride, glucose (from the lab), sodium, potassium, ph

DROP TABLE IF EXISTS `db_name.mimiciv_pulseOx.chemistry`;
CREATE TABLE `db_name.mimiciv_pulseOx.chemistry` AS

WITH 

  albumin AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , albumin
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(chem.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_albumin
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, chem.charttime, MINUTE)) ASC) AS seq

    FROM `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.chemistry` 
    AS chem
    ON chem.subject_id = pairs.subject_id
    AND albumin IS NOT NULL
    AND TIMESTAMP_DIFF(pairs.SaO2_timestamp, chem.charttime,MINUTE) >= 0

  ) 
  WHERE seq = 1

)

, aniongap AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , aniongap
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(chem.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_aniongap
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, chem.charttime, MINUTE)) ASC) AS seq

    FROM `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.chemistry` 
    AS chem
    ON chem.subject_id = pairs.subject_id
    AND aniongap IS NOT NULL
    AND TIMESTAMP_DIFF(pairs.SaO2_timestamp, chem.charttime,MINUTE) >= 0

  ) 
  WHERE seq = 1

)

, bicarbonate AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , bicarbonate
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(chem.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_bicarbonate
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, chem.charttime, MINUTE)) ASC) AS seq

    FROM `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.chemistry` 
    AS chem
    ON chem.subject_id = pairs.subject_id
    AND bicarbonate IS NOT NULL
    AND TIMESTAMP_DIFF(pairs.SaO2_timestamp, chem.charttime,MINUTE) >= 0

  ) 
  WHERE seq = 1

)

, bun AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , bun
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(chem.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_bun
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, chem.charttime, MINUTE)) ASC) AS seq

    FROM `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.chemistry` 
    AS chem
    ON chem.subject_id = pairs.subject_id
    AND bun IS NOT NULL
    AND TIMESTAMP_DIFF(pairs.SaO2_timestamp, chem.charttime,MINUTE) >= 0

  ) 
  WHERE seq = 1

)

, calcium AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , calcium
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(chem.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_calcium
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, chem.charttime, MINUTE)) ASC) AS seq

    FROM `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.chemistry` 
    AS chem
    ON chem.subject_id = pairs.subject_id
    AND calcium IS NOT NULL
    AND TIMESTAMP_DIFF(pairs.SaO2_timestamp, chem.charttime,MINUTE) >= 0

  ) 
  WHERE seq = 1

)

, chloride AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , chloride
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(chem.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_chloride
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, chem.charttime, MINUTE)) ASC) AS seq

    FROM `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.chemistry` 
    AS chem
    ON chem.subject_id = pairs.subject_id
    AND chloride IS NOT NULL
    AND TIMESTAMP_DIFF(pairs.SaO2_timestamp, chem.charttime,MINUTE) >= 0

  ) 
  WHERE seq = 1

)

, creatinine AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , creatinine
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(chem.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_creatinine
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, chem.charttime, MINUTE)) ASC) AS seq

    FROM `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.chemistry` 
    AS chem
    ON chem.subject_id = pairs.subject_id
    AND creatinine IS NOT NULL
    AND TIMESTAMP_DIFF(pairs.SaO2_timestamp, chem.charttime,MINUTE) >= 0

  ) 
  WHERE seq = 1

)

, glucose_lab AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , glucose AS glucose_lab
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(chem.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_glucose_lab
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, chem.charttime, MINUTE)) ASC) AS seq

    FROM `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.chemistry` 
    AS chem
    ON chem.subject_id = pairs.subject_id
    AND glucose IS NOT NULL
    AND TIMESTAMP_DIFF(pairs.SaO2_timestamp, chem.charttime,MINUTE) >= 0

  ) 
  WHERE seq = 1

)

, sodium AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , sodium
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(chem.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_sodium
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, chem.charttime, MINUTE)) ASC) AS seq

    FROM `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.chemistry` 
    AS chem
    ON chem.subject_id = pairs.subject_id
    AND sodium IS NOT NULL
    AND TIMESTAMP_DIFF(pairs.SaO2_timestamp, chem.charttime,MINUTE) >= 0

  ) 
  WHERE seq = 1

)

, potassium AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , potassium
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(chem.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_potassium
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, chem.charttime, MINUTE)) ASC) AS seq

    FROM `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.chemistry` 
    AS chem
    ON chem.subject_id = pairs.subject_id
    AND potassium IS NOT NULL
    AND TIMESTAMP_DIFF(pairs.SaO2_timestamp, chem.charttime,MINUTE) >= 0

  ) 
  WHERE seq = 1

)

, ph AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , ph
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(bg.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_ph
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, bg.charttime, MINUTE)) ASC) AS seq

    FROM `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.bg` 
    AS bg
    ON bg.subject_id = pairs.subject_id
    AND ph IS NOT NULL
    AND TIMESTAMP_DIFF(pairs.SaO2_timestamp, bg.charttime,MINUTE) >= 0

  ) 
  WHERE seq = 1

)

, lactate AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , lactate
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(bg.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_lactate
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, bg.charttime, MINUTE)) ASC) AS seq

    FROM `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.bg` 
    AS bg
    ON bg.subject_id = pairs.subject_id
    AND lactate IS NOT NULL
    AND TIMESTAMP_DIFF(pairs.SaO2_timestamp, bg.charttime,MINUTE) >= 0

  ) 
  WHERE seq = 1

)

SELECT 
    pairs.subject_id
  , pairs.stay_id
  , pairs.SaO2_timestamp
  , albumin.delta_albumin
  , albumin.albumin
  , aniongap.delta_aniongap
  , aniongap.aniongap
  , bicarbonate.delta_bicarbonate
  , bicarbonate.bicarbonate
  , bun.delta_bun
  , bun.bun
  , calcium.delta_calcium
  , calcium.calcium
  , chloride.delta_chloride
  , chloride.chloride
  , creatinine.delta_creatinine
  , creatinine.creatinine
  , glucose_lab.delta_glucose_lab
  , glucose_lab.glucose_lab
  , sodium.delta_sodium
  , sodium.sodium
  , potassium.delta_potassium
  , potassium.potassium
  , ph.delta_ph
  , ph.ph
  , lactate.delta_lactate
  , lactate.lactate

FROM `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

LEFT JOIN albumin
ON albumin.subject_id = pairs.subject_id
AND albumin.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN aniongap
ON aniongap.subject_id = pairs.subject_id
AND aniongap.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN bicarbonate
ON bicarbonate.subject_id = pairs.subject_id
AND bicarbonate.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN bun
ON bun.subject_id = pairs.subject_id
AND bun.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN calcium
ON calcium.subject_id = pairs.subject_id
AND calcium.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN chloride
ON chloride.subject_id = pairs.subject_id
AND chloride.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN creatinine
ON creatinine.subject_id = pairs.subject_id
AND creatinine.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN glucose_lab
ON glucose_lab.subject_id = pairs.subject_id
AND glucose_lab.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN sodium
ON sodium.subject_id = pairs.subject_id
AND sodium.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN potassium
ON potassium.subject_id = pairs.subject_id
AND potassium.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN ph
ON ph.subject_id = pairs.subject_id
AND ph.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN lactate
ON lactate.subject_id = pairs.subject_id
AND lactate.SaO2_timestamp = pairs.SaO2_timestamp

ORDER BY subject_id, stay_id, SaO2_timestamp