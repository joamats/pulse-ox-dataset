WITH pairs AS (
  SELECT
    sao2_table.patientunitstayid
  , sao2_table.labresultoffset AS SaO2_offset

  FROM `physionet-data.eicu_crd.lab` AS sao2_table

  -- Only for SaO2 values
  WHERE sao2_table.labname = "O2 Sat (%)"  -- SaO2
  AND sao2_table.labresultoffset > 0 -- positive values that refer to this stay
  AND sao2_table.labresultoffset IS NOT NULL

  GROUP BY patientunitstayid, SaO2_offset
  ORDER BY SaO2_offset ASC
)


SELECT DISTINCT
    pairs.patientunitstayid
  , SaO2_offset
  , sao2_vals.labResult AS SaO2 -- use Lab Result to Get NUMERIC values only

FROM pairs

LEFT JOIN `physionet-data.eicu_crd.lab`
AS sao2_vals
ON sao2_vals.patientunitstayid = pairs.patientunitstayid
AND sao2_vals.labresultoffset = pairs.SaO2_offset
AND sao2_vals.labname = "O2 Sat (%)"  -- SaO2

ORDER BY patientunitstayid, SaO2_offset ASC