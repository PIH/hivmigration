delete from encounter_provider where encounter_id in ( select encounter_id from encounter where patient_id in (select person_id from hivmigration_infants));
delete from encounter_provider where encounter_id in ( select encounter_id from encounter where patient_id in (select person_id from hivmigration_patients));
delete from encounter where patient_id in (select person_id from hivmigration_infants);
delete from encounter where patient_id in (select person_id from hivmigration_patients);
