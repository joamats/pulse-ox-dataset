DROP TABLE IF EXISTS `db_name.mimiciii_pulseOx.patient_ICU`;
CREATE TABLE `db_name.mimiciii_pulseOx.patient_ICU` AS

SELECT DISTINCT
icu.subject_id
, icu.icustay_id
, icu.hadm_id
, icu.gender
, CASE WHEN icu.gender  =  "F" THEN 1 ELSE 0 END AS sex_female
, icu.admission_age
, icu.first_hosp_stay
, icu.first_icu_stay
, icu.admittime
, icu.dischtime
, icu.intime as icu_intime
, icu.outtime as icu_outtime
, icu.los_hospital
, icu.los_icu
, icu.icustay_seq
, sf.sofa
, ad.language
, ad.insurance
, heightweight.weight_first as weight
, heightweight.height_first as height
, heightweight.weight_first / (POWER(heightweight.height_first/100, 2)) AS BMI
, icu.ethnicity
, CASE WHEN icu.ethnicity IN
  (
       'WHITE' --  40996
     , 'WHITE - RUSSIAN' --    164
     , 'WHITE - OTHER EUROPEAN' --     81
     , 'WHITE - BRAZILIAN' --     59
     , 'WHITE - EASTERN EUROPEAN' --     25
     , 'PORTUGUESE' --     61
  ) THEN 'White'
  WHEN icu.ethnicity IN
  (
      'BLACK/AFRICAN AMERICAN' --   5440
    , 'BLACK/CAPE VERDEAN' --    200
    , 'BLACK/HAITIAN' --    101
    , 'BLACK/AFRICAN' --     44
    , 'CARIBBEAN ISLAND' --      9
  ) THEN 'Black'
  WHEN icu.ethnicity IN
    (
      'HISPANIC OR LATINO' --   1696
    , 'HISPANIC/LATINO - PUERTO RICAN' --    232
    , 'HISPANIC/LATINO - DOMINICAN' --     78
    , 'HISPANIC/LATINO - GUATEMALAN' --     40
    , 'HISPANIC/LATINO - CUBAN' --     24
    , 'HISPANIC/LATINO - SALVADORAN' --     19
    , 'HISPANIC/LATINO - CENTRAL AMERICAN (OTHER)' --     13
    , 'HISPANIC/LATINO - MEXICAN' --     13
    , 'HISPANIC/LATINO - COLOMBIAN' --      9
    , 'HISPANIC/LATINO - HONDURAN' --      4
    , 'SOUTH AMERICAN' --      8
  ) THEN 'Hispanic'
  WHEN icu.ethnicity IN
  (
      'ASIAN' --   1509
    , 'ASIAN - CHINESE' --    277
    , 'ASIAN - ASIAN INDIAN' --     85
    , 'ASIAN - VIETNAMESE' --     53
    , 'ASIAN - FILIPINO' --     25
    , 'ASIAN - CAMBODIAN' --     17
    , 'ASIAN - OTHER' --     17
    , 'ASIAN - KOREAN' --     13
    , 'ASIAN - JAPANESE' --      7
    , 'ASIAN - THAI' --      4
  ) THEN 'Asian'
  ELSE 'Other' END AS race_group

  , elixhauser_vanwalraven AS elixhauser
  , details.hospital_expire_flag AS mortality_in


FROM `physionet-data.mimiciii_clinical.icustays`
AS stays

-- ICU stays
LEFT JOIN physionet-data.mimiciii_derived.icustay_detail
AS icu
ON stays.subject_id = icu.subject_id 
AND stays.hadm_id = icu.hadm_id 
AND stays.icustay_id = icu.icustay_id

-- Hospital Admissions
LEFT JOIN physionet-data.mimiciii_clinical.admissions
AS ad
ON ad.hadm_id = icu.hadm_id

-- Height and Weight
LEFT JOIN physionet-data.mimiciii_derived.heightweight
AS heightweight
ON heightweight.icustay_id = icu.icustay_id

-- SOFA
LEFT JOIN physionet-data.mimiciii_derived.sofa
AS sf
ON sf.icustay_id = icu.icustay_id

-- Elixhauser
LEFT JOIN physionet-data.mimiciii_derived.elixhauser_quan_score
AS elix
ON elix.hadm_id = icu.hadm_id

-- Derived ICU Stay Detail
LEFT JOIN physionet-data.mimiciii_derived.icustay_detail
AS details
ON details.icustay_id = icu.icustay_id
