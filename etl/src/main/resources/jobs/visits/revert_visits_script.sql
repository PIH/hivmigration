
delete from visit where patient_id in (select person_id from hivmigration_patients);
