drop table if exists hivmigration_programs;

create table hivmigration_programs (
    patient_program_id int PRIMARY KEY AUTO_INCREMENT,
	source_patient_id int,
    location_id int,
    enrollment_date date,
    art_start_date date,
    outcome_date date,
    outcome varchar(255),
    omrs_patient_id int,
    omrs_location_id int
);
