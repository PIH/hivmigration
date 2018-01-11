drop table if exists hivmigration_encounters;

create table hivmigration_encounters (
  source_encounter_id int,
	source_patient_id int,
  encounter_uuid char(38),
  visit_uuid char(38),
  encounter_type varchar(100),
  encounter_date date,
  performed_by varchar(100),
  source_location_id int,
  created_by int,
  created_date timestamp
);