drop table if exists hivmigration_lab_results;

create table hivmigration_lab_results (
  obs_id int PRIMARY KEY AUTO_INCREMENT,
  source_patient_id int,
  source_encounter_id int,
  source_result_id int,
  sample_id VARCHAR(20),
  test_type VARCHAR(16), -- viral_load, CD4, tr, ppd, hematocrit
  test_name VARCHAR(16), -- ExaVir, other
  obs_datetime date,
  value_numeric DOUBLE,
  value_text VARCHAR(100),
  value_boolean BOOLEAN,
  vl_beyond_detectable_limit BOOLEAN,
  vl_detectable_lower_limit DOUBLE,
  uuid char(38),
  KEY `source_encounter_id_idx` (`source_encounter_id`)
);

