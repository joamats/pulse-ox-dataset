-- enzyme Values include liver/abdominal markers from the lab
-- alt, alp, ast, bilirubin_total, bilirubin_direct, bilirubin_indirect, ck_cpk, ck_mb, ggt, ld_ldh

DROP TABLE IF EXISTS `db_name.mimiciv_pulseOx.enzyme`;
CREATE TABLE `db_name.mimiciv_pulseOx.enzyme` AS

WITH 

  alt AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , alt
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(enz.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_alt
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, enz.charttime, MINUTE)) ASC) AS seq

    FROM `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.enzyme` 
    AS enz
    ON enz.subject_id = pairs.subject_id
    AND alt IS NOT NULL
    AND TIMESTAMP_DIFF(pairs.SaO2_timestamp, enz.charttime,MINUTE) >= 0

  ) 
  WHERE seq = 1

)

, alp AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , alp
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(enz.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_alp
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, enz.charttime, MINUTE)) ASC) AS seq

    FROM `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.enzyme` 
    AS enz
    ON enz.subject_id = pairs.subject_id
    AND alp IS NOT NULL
    AND TIMESTAMP_DIFF(pairs.SaO2_timestamp, enz.charttime,MINUTE) >= 0

  ) 
  WHERE seq = 1

)

, ast AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , ast
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(enz.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_ast
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, enz.charttime, MINUTE)) ASC) AS seq

    FROM `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.enzyme` 
    AS enz
    ON enz.subject_id = pairs.subject_id
    AND ast IS NOT NULL
    AND TIMESTAMP_DIFF(pairs.SaO2_timestamp, enz.charttime,MINUTE) >= 0

  ) 
  WHERE seq = 1

)

, bilirubin_total AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , bilirubin_total
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(enz.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_bilirubin_total
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, enz.charttime, MINUTE)) ASC) AS seq

    FROM `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.enzyme` 
    AS enz
    ON enz.subject_id = pairs.subject_id
    AND bilirubin_total IS NOT NULL
    AND TIMESTAMP_DIFF(pairs.SaO2_timestamp, enz.charttime,MINUTE) >= 0

  ) 
  WHERE seq = 1

)

, bilirubin_direct AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , bilirubin_direct
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(enz.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_bilirubin_direct
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, enz.charttime, MINUTE)) ASC) AS seq

    FROM `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.enzyme` 
    AS enz
    ON enz.subject_id = pairs.subject_id
    AND bilirubin_direct IS NOT NULL
    AND TIMESTAMP_DIFF(pairs.SaO2_timestamp, enz.charttime,MINUTE) >= 0


  ) 
  WHERE seq = 1

)

, bilirubin_indirect AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , bilirubin_indirect
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(enz.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_bilirubin_indirect
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, enz.charttime, MINUTE)) ASC) AS seq

    FROM `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.enzyme` 
    AS enz
    ON enz.subject_id = pairs.subject_id
    AND bilirubin_indirect IS NOT NULL
    AND TIMESTAMP_DIFF(pairs.SaO2_timestamp, enz.charttime,MINUTE) >= 0


  ) 
  WHERE seq = 1

)

, ck_cpk AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , ck_cpk
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(enz.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_ck_cpk
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, enz.charttime, MINUTE)) ASC) AS seq

    FROM `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.enzyme` 
    AS enz
    ON enz.subject_id = pairs.subject_id
    AND ck_cpk IS NOT NULL
    AND TIMESTAMP_DIFF(pairs.SaO2_timestamp, enz.charttime,MINUTE) >= 0

  ) 
  WHERE seq = 1

)

, ck_mb AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , ck_mb AS ck_mb
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(enz.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_ck_mb
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, enz.charttime, MINUTE)) ASC) AS seq

    FROM `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.enzyme` 
    AS enz
    ON enz.subject_id = pairs.subject_id
    AND ck_mb IS NOT NULL
    AND TIMESTAMP_DIFF(pairs.SaO2_timestamp, enz.charttime,MINUTE) >= 0

  ) 
  WHERE seq = 1

)

, ggt AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , ggt
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(enz.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_ggt
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, enz.charttime, MINUTE)) ASC) AS seq

    FROM `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.enzyme` 
    AS enz
    ON enz.subject_id = pairs.subject_id
    AND ggt IS NOT NULL
    AND TIMESTAMP_DIFF(pairs.SaO2_timestamp, enz.charttime,MINUTE) >= 0

  ) 
  WHERE seq = 1

)

, ld_ldh AS (
  SELECT * FROM(
    SELECT
      pairs.subject_id
    , ld_ldh
    , pairs.SaO2_timestamp
    , TIMESTAMP_DIFF(enz.charttime, pairs.SaO2_timestamp, MINUTE) AS delta_ld_ldh
    , ROW_NUMBER() OVER(PARTITION BY pairs.subject_id, pairs.SaO2_timestamp
                        ORDER BY ABS(TIMESTAMP_DIFF(pairs.SaO2_timestamp, enz.charttime, MINUTE)) ASC) AS seq

    FROM `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

    LEFT JOIN `physionet-data.mimiciv_derived.enzyme` 
    AS enz
    ON enz.subject_id = pairs.subject_id
    AND ld_ldh IS NOT NULL
    AND TIMESTAMP_DIFF(pairs.SaO2_timestamp, enz.charttime,MINUTE) >= 0

  ) 
  WHERE seq = 1

)


SELECT 
    pairs.subject_id
  , pairs.stay_id
  , pairs.SaO2_timestamp
  , alt.delta_alt
  , alt.alt
  , alp.delta_alp
  , alp.alp
  , ast.delta_ast
  , ast.ast
  , bilirubin_total.delta_bilirubin_total
  , bilirubin_total.bilirubin_total
  , bilirubin_direct.delta_bilirubin_direct
  , bilirubin_direct.bilirubin_direct
  , bilirubin_indirect.delta_bilirubin_indirect
  , bilirubin_indirect.bilirubin_indirect
  , ck_cpk.delta_ck_cpk
  , ck_cpk.ck_cpk
  , ck_mb.delta_ck_mb
  , ck_mb.ck_mb
  , ggt.delta_ggt
  , ggt.ggt
  , ld_ldh.delta_ld_ldh
  , ld_ldh.ld_ldh

FROM `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

LEFT JOIN alt
ON alt.subject_id = pairs.subject_id
AND alt.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN alp
ON alp.subject_id = pairs.subject_id
AND alp.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN ast
ON ast.subject_id = pairs.subject_id
AND ast.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN bilirubin_total
ON bilirubin_total.subject_id = pairs.subject_id
AND bilirubin_total.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN bilirubin_direct
ON bilirubin_direct.subject_id = pairs.subject_id
AND bilirubin_direct.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN bilirubin_indirect
ON bilirubin_indirect.subject_id = pairs.subject_id
AND bilirubin_indirect.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN ck_cpk
ON ck_cpk.subject_id = pairs.subject_id
AND ck_cpk.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN ck_mb
ON ck_mb.subject_id = pairs.subject_id
AND ck_mb.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN ggt
ON ggt.subject_id = pairs.subject_id
AND ggt.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN ld_ldh
ON ld_ldh.subject_id = pairs.subject_id
AND ld_ldh.SaO2_timestamp = pairs.SaO2_timestamp

ORDER BY subject_id, stay_id, SaO2_timestamp