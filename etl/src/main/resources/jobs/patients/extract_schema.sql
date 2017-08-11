drop table if exists hivmigration_patients;

create table hivmigration_patients (
	source_patient_id int,
  person_uuid char(38),
	pih_id varchar(100),
	nif_id varchar(100),
	national_id varchar(100),
	first_name varchar(100),
	first_name2 varchar(100),
	last_name varchar(100),
	gender varchar(50),
	birthdate date,
	birthdate_estimated tinyint,
	phone_number varchar(100),
  birth_place varchar(255),
  accompagnateur_name varchar(255),
  patient_created_by int,
  patient_created_date datetime
);
