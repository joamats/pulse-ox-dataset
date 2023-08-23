-- Generate our own lab table based on LCP code

DROP TABLE IF EXISTS `protean-chassis-368116.eicu1_pulseOx.pivoted_lab_extended`;
CREATE TABLE `protean-chassis-368116.eicu1_pulseOx.pivoted_lab_extended` AS

-- not exisitng in eICU
-- d_dimer
-- thrombin
-- indirect bilirubin
-- ggt

-- remove duplicate labs if they exist at the same time
with vw0 as
(
  select
      patientunitstayid
    , labname
    , labresultoffset
    , labresultrevisedoffset
  FROM `physionet-data.eicu_crd.lab` AS lab
  where labname in
  (
      'albumin'
    , 'total bilirubin'
    , 'direct bilirubin'
    , 'BUN'
    , 'calcium'
    , 'chloride'
    , 'creatinine'
    , 'bedside glucose', 'glucose'
    , 'bicarbonate' -- HCO3
    , 'Total CO2'
    , 'Hct'
    , 'Hgb'
    , 'PT - INR'
    , 'PTT'
    , 'lactate'
    , 'platelets x 1000'
    , 'potassium'
    , 'sodium'
    , 'WBC x 1000'
    , '-bands'
    -- Liver enzymes
    , 'ALT (SGPT)'
    , 'AST (SGOT)'
    , 'alkaline phos.'
    , 'fibrinogen'
    , 'PT'
    , 'MCH'
    , 'MCHC'
    , 'MCV'
    , 'MPV'
    , 'RDW'
    , 'RBC'
    , 'CPK'
    , 'CPK-MB'
    , 'LDH'
    , 'anion gap'
  )
  group by patientunitstayid, labname, labresultoffset, labresultrevisedoffset
  having count(distinct labresult)<=1
)



-- get the last lab to be revised
, vw1 as
(
  select
      lab.patientunitstayid
    , lab.labname
    , lab.labresultoffset
    , lab.labresultrevisedoffset
    , lab.labresult
    , ROW_NUMBER() OVER
        (
          PARTITION BY lab.patientunitstayid, lab.labname, lab.labresultoffset
          ORDER BY lab.labresultrevisedoffset DESC
        ) as rn
  FROM `physionet-data.eicu_crd.lab` AS lab
  inner join vw0
    ON  lab.patientunitstayid = vw0.patientunitstayid
    AND lab.labname = vw0.labname
    AND lab.labresultoffset = vw0.labresultoffset
    AND lab.labresultrevisedoffset = vw0.labresultrevisedoffset
  -- only valid lab values
  WHERE
       (lab.labname = 'albumin' and lab.labresult >= 0.5 and lab.labresult <= 6.5)
    OR (lab.labname = 'total bilirubin' and lab.labresult >= 0.2 and lab.labresult <= 100)
    OR (lab.labname = 'direct bilirubin' and lab.labresult >= 0.01 and lab.labresult <= 80)
    OR (lab.labname = 'BUN' and lab.labresult >= 1 and lab.labresult <= 280)
    OR (lab.labname = 'calcium' and lab.labresult > 0 and lab.labresult <= 9999)
    OR (lab.labname = 'chloride' and lab.labresult > 0 and lab.labresult <= 9999)
    OR (lab.labname = 'creatinine' and lab.labresult >= 0.1 and lab.labresult <= 28.28)
    OR (lab.labname in ('bedside glucose', 'glucose') and lab.labresult >= 25 and lab.labresult <= 1500)
    OR (lab.labname = 'bicarbonate' and lab.labresult >= 0 and lab.labresult <= 9999)
    OR (lab.labname = 'Total CO2' and lab.labresult >= 0 and lab.labresult <= 9999)
    -- will convert hct unit to fraction later
    OR (lab.labname = 'Hct' and lab.labresult >= 5 and lab.labresult <= 75)
    OR (lab.labname = 'Hgb' and lab.labresult >  0 and lab.labresult <= 9999)
    OR (lab.labname = 'PT - INR' and lab.labresult >= 0.5 and lab.labresult <= 15)
    OR (lab.labname = 'lactate' and lab.labresult >= 0.1 and lab.labresult <= 30)
    OR (lab.labname = 'platelets x 1000' and lab.labresult >  0 and lab.labresult <= 9999)
    OR (lab.labname = 'potassium' and lab.labresult >= 0.05 and lab.labresult <= 12)
    OR (lab.labname = 'PTT' and lab.labresult >  0 and lab.labresult <= 500)
    OR (lab.labname = 'sodium' and lab.labresult >= 90 and lab.labresult <= 215)
    OR (lab.labname = 'WBC x 1000' and lab.labresult > 0 and lab.labresult <= 100)
    OR (lab.labname = '-bands' and lab.labresult >= 0 and lab.labresult <= 100)
    OR (lab.labname = 'ALT (SGPT)' and lab.labresult > 0)
    OR (lab.labname = 'AST (SGOT)' and lab.labresult > 0)
    OR (lab.labname = 'alkaline phos.' and lab.labresult > 0)
    OR (lab.labname = 'fibrinogen' and lab.labresult IS NOT NULL)
    OR (lab.labname = 'PT' and lab.labresult > 0)
    OR (lab.labname = 'MCH' and lab.labresult IS NOT NULL)
    OR (lab.labname = 'MCHC' and lab.labresult IS NOT NULL)
    OR (lab.labname = 'MCV' and lab.labresult IS NOT NULL)
    OR (lab.labname = 'MPV' and lab.labresult IS NOT NULL)
    OR (lab.labname = 'RDW' and lab.labresult IS NOT NULL)
    OR (lab.labname = 'RBC' and lab.labresult IS NOT NULL)
    OR (lab.labname = 'CPK' and lab.labresult > 0)
    OR (lab.labname = 'CPK-MB' and lab.labresult > 0)
    OR (lab.labname = 'LDH' and lab.labresult IS NOT NULL)
    OR (lab.labname = 'anion gap' and lab.labresult IS NOT NULL)
)

select
    patientunitstayid
  , labresultoffset as chartoffset
  , MAX(case when labname = 'albumin' then labresult else null end) as albumin
  , MAX(case when labname = 'total bilirubin' then labresult else null end) as bilirubin_total
  , MAX(case when labname = 'direct bilirubin' then labresult else null end) as bilirubin_direct
  , MAX(case when labname = 'BUN' then labresult else null end) as bun
  , MAX(case when labname = 'calcium' then labresult else null end) as calcium
  , MAX(case when labname = 'chloride' then labresult else null end) as chloride
  , MAX(case when labname = 'creatinine' then labresult else null end) as creatinine
  , MAX(case when labname in ('bedside glucose', 'glucose') then labresult else null end) as glucose
  , MAX(case when labname = 'bicarbonate' then labresult else null end) as bicarbonate
  , MAX(case when labname = 'Total CO2' then labresult else null end) as TotalCO2
  , MAX(case when labname = 'Hct' then labresult else null end) as hematocrit
  , MAX(case when labname = 'Hgb' then labresult else null end) as hemoglobin
  , MAX(case when labname = 'PT - INR' then labresult else null end) as INR
  , MAX(case when labname = 'lactate' then labresult else null end) as lactate
  , MAX(case when labname = 'platelets x 1000' then labresult else null end) as platelets
  , MAX(case when labname = 'potassium' then labresult else null end) as potassium
  , MAX(case when labname = 'PTT' then labresult else null end) as ptt
  , MAX(case when labname = 'PT' then labresult else null end) as pt
  , MAX(case when labname = 'sodium' then labresult else null end) as sodium
  , MAX(case when labname = 'WBC x 1000' then labresult else null end) as wbc
  , MAX(case when labname = '-bands' then labresult else null end) as bands
  , MAX(case when labname = 'ALT (SGPT)' then labresult else null end) as alt
  , MAX(case when labname = 'AST (SGOT)' then labresult else null end) as ast
  , MAX(case when labname = 'alkaline phos.' then labresult else null end) as alp
  , MAX(case when labname = 'fibrinogen' then labresult else null end) as fibrinogen
  , MAX(case when labname = 'MCH' then labresult else null end) as mch
  , MAX(case when labname = 'MCHC' then labresult else null end) as mchc
  , MAX(case when labname = 'MCV' then labresult else null end) as mcv
  , MAX(case when labname = 'MPV' then labresult else null end) as mpv
  , MAX(case when labname = 'RDW' then labresult else null end) as rdw
  , MAX(case when labname = 'RBC' then labresult else null end) as rbc
  , MAX(case when labname = 'CPK' then labresult else null end) as ck_cpk
  , MAX(case when labname = 'CPK-MB' then labresult else null end) as ck_mb
  , MAX(case when labname = 'LDH' then labresult else null end) as ld_ldh
  , MAX(case when labname = 'anion gap' then labresult else null end) as aniongap

from vw1
where rn = 1
group by patientunitstayid, labresultoffset
order by patientunitstayid, labresultoffset;