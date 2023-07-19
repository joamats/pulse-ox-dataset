    -- This query pivots the vital signs for the first 24 hours of a patient's stay
-- Vital signs include heart rate, blood pressure, respiration rate, and temperature

drop table if exists `protean-chassis-368116.mimiciii_pulseOx.pivoted_vital_extended`;
create table `protean-chassis-368116.mimiciii_pulseOx.pivoted_vital_extended` as

with ce as
(
  select ce.icustay_id
    , ce.charttime
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

  442, --	Manual BP [Systolic]
  455, --	NBP [Systolic]
  220179, --	Non Invasive Blood Pressure systolic

  8440, --	Manual BP [Diastolic]
  8441, --	NBP [Diastolic]
  220180, --	Non Invasive Blood Pressure diastolic


  -- MEAN ARTERIAL PRESSURE
  456, --"NBP Mean"
  443, --	Manual BP Mean(calc)
  220181 --"Non Invasive Blood Pressure mean"

  )
)
select
    ce.icustay_id
  , ce.charttime
  , avg(sysbp_ni) as sysbp_ni
  , avg(diasbp_ni) as diasbp_ni
  , avg(meanbp_ni) as meanbp_ni
from ce
group by ce.icustay_id, ce.charttime
order by ce.icustay_id, ce.charttime;