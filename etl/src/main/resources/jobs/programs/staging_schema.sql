drop table if exists hivmigration_programs;

create table hivmigration_programs (
	source_patient_id int,
  location_id int,
  enrollment_date date,
  art_start_date date,
  outcome_date date,
  outcome varchar(255)
);
