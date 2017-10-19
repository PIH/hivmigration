drop table if exists hivmigration_infants;

create table hivmigration_infants (
  person_id int PRIMARY KEY AUTO_INCREMENT,
	source_infant_id int,
	mother_patient_id int,
    person_uuid char(38),
	infant_code varchar(20),
	first_name varchar(100),
	last_name varchar(100),
	gender varchar(50),
	birthdate date
);
