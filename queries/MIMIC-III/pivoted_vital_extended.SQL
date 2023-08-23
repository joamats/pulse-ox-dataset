    -- This query pivots the vital signs for the first 24 hours of a patient's stay
-- Vital signs include heart rate, blood pressure, respiration rate, and temperature

drop table if exists `protean-chassis-368116.mimiciii_pulseOx.pivoted_vital_extended`;
create table `protean-chassis-368116.mimiciii_pulseOx.pivoted_vital_extended` as

with ce as
(
  select ce.icustay_id
    , ce.charttime
    , (case when itemid in (51,6701,220050) and valuenum > 0 and valuenum < 400 then valuenum else null end) as sysbp_i
    , (case when itemid in (8368,8555,220051) and valuenum > 0 and valuenum < 300 then valuenum else null end) as diasbp_i
    , (case when itemid in (52,6702,220052,225312) and valuenum > 0 and valuenum < 300 then valuenum else null end) as meanbp_i

    , (case when itemid in (442,455,220179) and valuenum > 0 and valuenum < 400 then valuenum else null end) as sysbp_ni
    , (case when itemid in (8440,8441,220180) and valuenum > 0 and valuenum < 300 then valuenum else null end) as diasbp_ni
    , (case when itemid in (456,443,220181) and valuenum > 0 and valuenum < 300 then valuenum else null end) as meanbp_ni
  FROM `physionet-data.mimiciii_clinical.chartevents` ce
  -- exclude rows marked as error
  where (ce.error IS NULL OR ce.error != 1)
  and ce.icustay_id IS NOT NULL
  and ce.itemid in
  (
  -- Systolic/diastolic

  51, --	Arterial BP [Systolic]
  442, --	Manual BP [Systolic]
  455, --	NBP [Systolic]
  6701, --	Arterial BP #2 [Systolic]
  220179, --	Non Invasive Blood Pressure systolic
  220050, --	Arterial Blood Pressure systolic

  8368, --	Arterial BP [Diastolic]
  8440, --	Manual BP [Diastolic]
  8441, --	NBP [Diastolic]
  8555, --	Arterial BP #2 [Diastolic]
  220180, --	Non Invasive Blood Pressure diastolic
  220051, --	Arterial Blood Pressure diastolic


  -- MEAN ARTERIAL PRESSURE
  456, --"NBP Mean"
  52, --"Arterial BP Mean"
  6702, --	Arterial BP Mean #2
  443, --	Manual BP Mean(calc)
  220052, --"Arterial Blood Pressure mean"
  220181, --"Non Invasive Blood Pressure mean"
  225312 --"ART BP mean"

  )
)
select
    ce.icustay_id
  , ce.charttime
  , avg(sysbp_ni) as sysbp_ni
  , avg(diasbp_ni) as diasbp_ni
  , avg(meanbp_ni) as meanbp_ni
  , avg(sysbp_i) as sysbp_i
  , avg(diasbp_i) as diasbp_i
  , avg(meanbp_i) as meanbp_i
from ce
group by ce.icustay_id, ce.charttime
order by ce.icustay_id, ce.charttime;