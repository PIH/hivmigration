drop table if exists hivmigration_observations;

create table hivmigration_observations (
  source_observation_id int,
  source_encounter_id int,
  observation varchar(100),
  value varchar(4000),
  entry_date date,
  observation_uuid char(38),
  KEY `source_observation_idx` (`source_observation_id`),
  KEY `source_encounter_idx` (`source_encounter_id`),
  UNIQUE KEY `uuid_idx` (`observation_uuid`)
);