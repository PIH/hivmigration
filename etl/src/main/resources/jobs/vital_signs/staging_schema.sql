drop table if exists hivmigration_vital_signs;

create table hivmigration_vital_signs (
  source_encounter_id int,
  sign varchar(30),
  result double,
  result_unit varchar(100),
  vital_sign_uuid char(38),
  KEY `source_encounter_idx` (`source_encounter_id`),
  UNIQUE KEY `uuid_idx` (`vital_sign_uuid`)
);