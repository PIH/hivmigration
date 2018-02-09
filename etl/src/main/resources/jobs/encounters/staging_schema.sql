drop table if exists hivmigration_encounters;

create table hivmigration_encounters (
  encounter_id int PRIMARY KEY AUTO_INCREMENT,
  patient_id int,
  source_creator_id int,
  source_encounter_id int,
  source_patient_id int,
  encounter_type_id int,
  source_encounter_type varchar(100),
  location_id int,
  source_location_id int,
  creator int,
  encounter_uuid char(38),
  encounter_date date,
  performed_by varchar(100),
  date_created datetime,
  comments varchar(4000),
  note_title varchar(100),
  response_to int,
  KEY `patient_id_idx` (`patient_id`),
  KEY `source_patient_id_idx` (`source_patient_id`),
  KEY `source_creator_id_idx` (`source_creator_id`),
  KEY `source_location_id_idx` (`source_location_id`),
  KEY `encounter_date_idx` (`encounter_date`),
  UNIQUE KEY `uuid_idx` (`encounter_uuid`)
);

# TODO: Do we want to include intake/follow-up form_version and link to form_id in OpenMRS?

/*
* Load visits based on these encounters (one visit per encounter date per patient for the below types)
  * For encounters of type (intake, followup, patient_contact), create "Facility Visit" at encounter_site
  * For encounters of type (accompagnateur), create "CHW Medication Pickup Visit" at encounter_site?
  * TBD:
    * "lab_result","160226"
    * "regime","104998"
    * "food_support","10144"
    * "note","4646"
    * "food_study","1479"
    * "hop_abstraction","711"
    * "anlap_lab_result","680"
    * "anlap_vital_signs","222"
    * "pregnancy","147"
    * "infant_followup","59"
    * "cervical_cancer","5"
* Load encounters
