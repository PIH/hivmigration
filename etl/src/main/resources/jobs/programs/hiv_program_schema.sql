drop table if exists hivmigration_program;

create table hivmigration_program (
  person_id int PRIMARY KEY AUTO_INCREMENT,
	source_patient_id int,
	first_name varchar(100),
	last_name varchar(100),
	gender varchar(50),
	birthdate date,
    health_center_id int,
    treatment_status varchar(32),
    treatment_status_date date,
    treatment_start_date date,
    starting_health_center_id int,
    health_center_transfer_date date
);
