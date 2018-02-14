CREATE PROCEDURE insert_vl_results ()

BEGIN

  DECLARE hiv_vl_construct int;
  DECLARE specimen_number int;
  DECLARE hvl_value int;
  DECLARE hvl_qualitative int;
  DECLARE not_detected int;
  DECLARE detectable_lower_limit int;
  DECLARE test_name int;
  DECLARE exa_vir int;

  -- HIV Viral Load Construct
  select concept_id INTO hiv_vl_construct from concept where uuid='11765b8c-a338-48a4-9480-df898c903723';

  -- add OBS_GROUP HIV Viral Load Construct
  insert into obs(obs_id, person_id, concept_id, encounter_id, obs_datetime, location_id, creator, date_created, voided, uuid)
    select r.obs_id, p.person_id, hiv_vl_construct, e.encounter_id, r.obs_datetime, e.location_id, 1, now(), 0, r.uuid
    from hivmigration_lab_results r join hivmigration_patients p on r.source_patient_id = p.source_patient_id
      join hivmigration_encounters e on r.source_encounter_id = e.source_encounter_id where r.test_type ='viral_load';

  -- Specimen Number
  select concept_id INTO specimen_number from concept where uuid='162086AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA';

  insert into obs(person_id, concept_id, encounter_id, obs_datetime, location_id, obs_group_id, value_text, creator, date_created, voided, uuid)
    select p.person_id, specimen_number, e.encounter_id, r.obs_datetime, e.location_id, r.obs_id, r.sample_id,1, now(), 0, uuid()
    from hivmigration_lab_results r join hivmigration_patients p on r.source_patient_id = p.source_patient_id
      join hivmigration_encounters e on r.source_encounter_id = e.source_encounter_id where r.test_type ='viral_load';


  -- HIV Viral Load Numeric Value
  select concept_id INTO hvl_value from concept where uuid='3cd4a882-26fe-102b-80cb-0017a47871b2';

  insert into obs(person_id, concept_id, encounter_id, obs_datetime, location_id, obs_group_id, value_numeric, creator, date_created, voided, uuid)
    select p.person_id, hvl_value, e.encounter_id, r.obs_datetime, e.location_id, r.obs_id, r.value_numeric,1, now(), 0, uuid()
    from hivmigration_lab_results r join hivmigration_patients p on r.source_patient_id = p.source_patient_id
      join hivmigration_encounters e on r.source_encounter_id = e.source_encounter_id
    where r.value_numeric is not null and r.test_type ='viral_load';

  -- HIV Viral Load Qualitative
  select concept_id INTO hvl_qualitative from concept where uuid='1305AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA';
  select concept_id INTO not_detected from concept where uuid='1302AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA';

  insert into obs(person_id, concept_id, encounter_id, obs_datetime, location_id, obs_group_id, value_coded, creator, date_created, voided, uuid)
    select p.person_id, hvl_qualitative, e.encounter_id, r.obs_datetime, e.location_id, r.obs_id, not_detected,1, now(), 0, uuid()
    from hivmigration_lab_results r join hivmigration_patients p on r.source_patient_id = p.source_patient_id
      join hivmigration_encounters e on r.source_encounter_id = e.source_encounter_id
    where r.vl_beyond_detectable_limit = 1 and r.test_type ='viral_load';

  -- Detectable Lower Limit
  select concept_id INTO detectable_lower_limit from concept where uuid='53cb83ed-5d55-4b63-922f-d6b8fc67a5f8';

  insert into obs(person_id, concept_id, encounter_id, obs_datetime, location_id, obs_group_id, value_numeric, creator, date_created, voided, uuid)
    select p.person_id, detectable_lower_limit, e.encounter_id, r.obs_datetime, e.location_id, r.obs_id, r.vl_detectable_lower_limit,1, now(), 0, uuid()
    from hivmigration_lab_results r join hivmigration_patients p on r.source_patient_id = p.source_patient_id
      join hivmigration_encounters e on r.source_encounter_id = e.source_encounter_id
    where r.vl_detectable_lower_limit is not null and r.test_type ='viral_load';

  -- HIV VL Test Name ExaVir
  select concept_id INTO test_name from concept where uuid='162087AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA';
  select concept_id INTO exa_vir from concept where uuid='6ECB7B1A-7010-4D29-8DBE-E883C2179068';

  insert into obs(person_id, concept_id, encounter_id, obs_datetime, location_id, obs_group_id, value_coded, creator, date_created, voided, uuid)
    select p.person_id, test_name, e.encounter_id, r.obs_datetime, e.location_id, r.obs_id, exa_vir,1, now(), 0, uuid()
    from hivmigration_lab_results r join hivmigration_patients p on r.source_patient_id = p.source_patient_id
      join hivmigration_encounters e on r.source_encounter_id = e.source_encounter_id
    where r.test_name = 'exavir' and r.test_type ='viral_load';

END