drop table if exists hivmigration_exam_extra;

create table hivmigration_exam_extra (
  source_encounter_id int,
  next_exam_date datetime,
  KEY `source_encounter_idx` (`source_encounter_id`),
  UNIQUE KEY `uuid_idx` (`exam_extra_uuid`)
);