delete from patient_identifier where patient_id in (select person_id from hivmigration_infants);
delete from patient where patient_id in (select person_id from hivmigration_infants);

delete from name_phonetics;
delete from person_name where person_id in (select person_id from hivmigration_infants);
delete from person where person_id in (select person_id from hivmigration_infants);

drop table hivmigration_infants;
