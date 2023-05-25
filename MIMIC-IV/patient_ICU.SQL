DROP TABLE IF EXISTS `db_name.mimiciv_pulseOx.patient_ICU`;
CREATE TABLE `db_name.mimiciv_pulseOx.patient_ICU` AS

SELECT DISTINCT
    icu.subject_id
  , icu.hadm_id
  , icu.stay_id
  , icu.gender
  , CASE WHEN icu.gender = "F" THEN 1 ELSE 0 END AS sex_female
  , pat.anchor_age
  , icu.race
  , CASE 
      WHEN (
         LOWER(icu.race) LIKE "%white%"
      OR LOWER(icu.race) LIKE "%portuguese%" 
      OR LOWER(icu.race) LIKE "%caucasian%" 
      ) THEN "White"
      WHEN (
         LOWER(icu.race) LIKE "%black%"
      OR LOWER(icu.race) LIKE "%african american%"
      ) THEN "Black"
      WHEN (
         LOWER(icu.race) LIKE "%hispanic%"
      OR LOWER(icu.race) LIKE "%south american%" 
      ) THEN "Hispanic"
      WHEN (
         LOWER(icu.race) LIKE "%asian%"
      ) THEN "Asian"
      ELSE "Other"
    END AS race_group
  , ad.language
  , ad.insurance
  , weight.weight_admit AS weight
  , height.height
  , weight.weight_admit / (POWER(height.height/100, 2)) AS BMI
  , pat.anchor_year_group
  , icu.first_hosp_stay
  , icu.first_icu_stay
  , icu.icustay_seq
  , icu.admittime
  , icu.dischtime
  , icu.icu_intime
  , icu.icu_outtime
  , icu.los_hospital
  , icu.los_icu
  , charlson.charlson_comorbidity_index AS CCI    
  , sf.SOFA AS SOFA_admission
  , CASE WHEN (
         discharge_location = "DIED"
      OR discharge_location = "HOSPICE"
  ) THEN 1
    ELSE 0
  END AS mortality_in

-- ICU stays
FROM physionet-data.mimiciv_derived.icustay_detail
AS icu 

-- Sepsis Patients
LEFT JOIN physionet-data.mimiciv_derived.sepsis3
AS s3
ON s3.stay_id = icu.stay_id

-- Age
LEFT JOIN physionet-data.mimiciv_hosp.patients
AS pat
ON icu.subject_id = pat.subject_id

-- SOFA
LEFT JOIN physionet-data.mimiciv_derived.first_day_sofa
AS sf
ON icu.stay_id = sf.stay_id 

-- Weight
LEFT JOIN physionet-data.mimiciv_derived.first_day_weight
AS weight
ON icu.stay_id = weight.stay_id 

-- Height
LEFT JOIN physionet-data.mimiciv_derived.first_day_height
AS height
ON icu.stay_id = height.stay_id 

-- Admissions
LEFT JOIN physionet-data.mimiciv_hosp.admissions
AS ad
ON icu.hadm_id = ad.hadm_id

-- Charlson 
LEFT JOIN physionet-data.mimiciv_derived.charlson
AS charlson
ON icu.hadm_id = charlson.hadm_id