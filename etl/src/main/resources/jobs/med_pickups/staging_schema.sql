drop table if exists hivmigration_medpickup;

create table hivmigration_medpickup (
  medpickup_id int PRIMARY KEY AUTO_INCREMENT,
  patient_id int,
  first_name varchar(100),
  last_name varchar(100),
  institution_id int,
  institution_name varchar(128),
  encounter_id int,
  encounter_date date,
  accompagnateur_name varchar(4000),
  accompagnateur_date_str varchar(20)
);
