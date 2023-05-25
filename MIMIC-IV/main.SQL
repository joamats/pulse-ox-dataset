SELECT * FROM `db_name.mimiciv_pulseOx.SaO2_SpO2_pairs` pairs

LEFT JOIN `db_name.mimiciv_pulseOx.patient_ICU` pat
USING (subject_id, stay_id)

LEFT JOIN `db_name.mimiciv_pulseOx.sofa` sofa
USING (subject_id, stay_id, SaO2_timestamp)

LEFT JOIN `db_name.mimiciv_pulseOx.blood_count` bld
USING (subject_id, stay_id, SaO2_timestamp)

LEFT JOIN `db_name.mimiciv_pulseOx.coagulation` coag
USING (subject_id, stay_id, SaO2_timestamp)

LEFT JOIN `db_name.mimiciv_pulseOx.enzyme` enz
USING (subject_id, stay_id, SaO2_timestamp)

LEFT JOIN `db_name.mimiciv_pulseOx.vital_signs` vitals
USING (subject_id, stay_id, SaO2_timestamp)
