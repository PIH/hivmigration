drop table if exists hivmigration_encounter_type;

create table hivmigration_encounter_type (
  id int PRIMARY KEY AUTO_INCREMENT,
	encounter_type varchar(255),
	encounter_type_id int
);

SET @encounter_type_intake = (select encounter_type_id from encounter_type where uuid = 'c31d306a-40c4-11e7-a919-92ebcb67fe33');
SET @encounter_type_followup = (select encounter_type_id from encounter_type where uuid = 'c31d3312-40c4-11e7-a919-92ebcb67fe33');
SET @encounter_type_specimen_collection = (select encounter_type_id from encounter_type where uuid = '10db3139-07c0-4766-b4e5-a41b01363145');
SET @encounter_type_medicaments_administres = (select encounter_type_id from encounter_type where uuid = '8ff50dea-18a1-4609-b4c9-3f8f2d611b84');

insert into hivmigration_encounter_type(encounter_type, encounter_type_id) values("intake", @encounter_type_intake);
insert into hivmigration_encounter_type(encounter_type, encounter_type_id) values("followup", @encounter_type_followup);
insert into hivmigration_encounter_type(encounter_type, encounter_type_id) values("lab_result", @encounter_type_specimen_collection);
insert into hivmigration_encounter_type(encounter_type, encounter_type_id) values("anlap_lab_result", @encounter_type_specimen_collection);
insert into hivmigration_encounter_type(encounter_type, encounter_type_id) values("accompagnateur", @encounter_type_medicaments_administres);

-- encounters with height/weight measurements
insert into hivmigration_encounter_type(encounter_type, encounter_type_id) values("anlap_vital_signs", @encounter_type_intake);
insert into hivmigration_encounter_type(encounter_type, encounter_type_id) values("patient_contact", @encounter_type_intake);
insert into hivmigration_encounter_type(encounter_type, encounter_type_id) values("food_study", @encounter_type_intake);
