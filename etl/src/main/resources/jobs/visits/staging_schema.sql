drop table if exists hivmigration_visits;

create table hivmigration_visits (
  visit_id int PRIMARY KEY AUTO_INCREMENT,
	patient_id int,
  visit_uuid char(38),
  encounter_datetime datetime,
  KEY `patient_id_idx` (`patient_id`),
  KEY `encounter_datetime_idx` (`encounter_datetime`),
  UNIQUE KEY `uuid_idx` (`visit_uuid`)
);
