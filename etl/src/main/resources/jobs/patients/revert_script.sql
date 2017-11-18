
delete from patient_identifier where patient_id in (select person_id from hivmigration_patients);
delete from patient where patient_id in (select person_id from hivmigration_patients);

delete from name_phonetics;
delete from obs where person_id in (select person_id from hivmigration_patients);
delete from person_attribute where person_id in (select person_id from hivmigration_patients);
delete from person_address where person_id in (select person_id from hivmigration_patients);
delete from person_name where person_id in (select person_id from hivmigration_patients);
delete from person where person_id in (select person_id from hivmigration_patients);

drop table hivmigration_patients;
