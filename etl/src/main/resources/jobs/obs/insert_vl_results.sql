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

  DECLARE cd4_construct int;
  DECLARE cd4_value int;
  DECLARE hematocrit_construct int;
  DECLARE hematocrit_value int;
  DECLARE ppd_construct int;
  DECLARE ppd_value int;
  DECLARE test_rapid_construct int;
  DECLARE positive int;
  DECLARE negative int;

  -- HIV Viral Load Construct
  select concept_id INTO hiv_vl_construct from concept where uuid='11765b8c-a338-48a4-9480-df898c903723';

  -- add OBS_GROUP HIV Viral Load Construct
  insert into obs(obs_id, person_id, concept_id, encounter_id, obs_datetime, location_id, creator, date_created, voided, uuid)
    select r.obs_id, p.person_id, hiv_vl_construct, e.encounter_id, r.obs_datetime, e.location_id, 1, now(), 0, r.uuid
    from hivmigration_lab_results r join hivmigration_patients p on r.source_patient_id = p.source_patient_id
      join hivmigration_encounters e on r.source_encounter_id = e.source_encounter_id where r.test_type ='viral_load';

  -- CD4 Count Construct
  select concept_id INTO @cd4_construct from concept where uuid='37769FDB-5FC1-4D47-82C2-DB88960BB224';

  -- add OBS_GROUP  CD4 Count Construct
  insert into obs(obs_id, person_id, concept_id, encounter_id, obs_datetime, location_id, creator, date_created, voided, uuid)
    select r.obs_id, p.person_id, @cd4_construct, e.encounter_id, r.obs_datetime, e.location_id, 1, now(), 0, r.uuid
    from hivmigration_lab_results r join hivmigration_patients p on r.source_patient_id = p.source_patient_id
      join hivmigration_encounters e on r.source_encounter_id = e.source_encounter_id where r.test_type ='cd4';

  -- hematocrit Construct
  select concept_id INTO @hematocrit_construct from concept where uuid='267C165C-1B8F-48FE-91AC-C1AE8C7412A0';

  -- add OBS_GROUP  hematocrit Construct
  insert into obs(obs_id, person_id, concept_id, encounter_id, obs_datetime, location_id, creator, date_created, voided, uuid)
    select r.obs_id, p.person_id, @hematocrit_construct, e.encounter_id, r.obs_datetime, e.location_id, 1, now(), 0, r.uuid
    from hivmigration_lab_results r join hivmigration_patients p on r.source_patient_id = p.source_patient_id
      join hivmigration_encounters e on r.source_encounter_id = e.source_encounter_id where r.test_type ='hematocrit';

  -- PPD Construct
  select concept_id INTO @ppd_construct from concept where uuid='ACF90DED-B595-4356-9840-788094C60AFB';

  -- add OBS_GROUP  PPD Construct
  insert into obs(obs_id, person_id, concept_id, encounter_id, obs_datetime, location_id, creator, date_created, voided, uuid)
    select r.obs_id, p.person_id, @ppd_construct, e.encounter_id, r.obs_datetime, e.location_id, 1, now(), 0, r.uuid
    from hivmigration_lab_results r join hivmigration_patients p on r.source_patient_id = p.source_patient_id
      join hivmigration_encounters e on r.source_encounter_id = e.source_encounter_id where r.test_type ='ppd';

  -- test_rapid Construct
  select concept_id INTO @test_rapid_construct from concept where uuid='A19F5A83-E960-413D-B93B-9270C53580A2';

  -- add OBS_GROUP  test_rapid Construct
  insert into obs(obs_id, person_id, concept_id, encounter_id, obs_datetime, location_id, creator, date_created, voided, uuid)
    select r.obs_id, p.person_id, @test_rapid_construct, e.encounter_id, r.obs_datetime, e.location_id, 1, now(), 0, r.uuid
    from hivmigration_lab_results r join hivmigration_patients p on r.source_patient_id = p.source_patient_id
      join hivmigration_encounters e on r.source_encounter_id = e.source_encounter_id where r.test_type ='tr';

  -- Finish loading viral load results
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

  -- Specimen Number
  select concept_id INTO @specimen_number from concept where uuid='162086AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA';

  insert into obs(person_id, concept_id, encounter_id, obs_datetime, location_id, obs_group_id, value_text, creator, date_created, voided, uuid)
    select p.person_id, @specimen_number, e.encounter_id, r.obs_datetime, e.location_id, r.obs_id, r.sample_id,1, now(), 0,  uuid()
    from hivmigration_lab_results r join hivmigration_patients p on r.source_patient_id = p.source_patient_id
      join hivmigration_encounters e on r.source_encounter_id = e.source_encounter_id where r.test_type ='cd4';


  -- CD4 Count Numeric Value
  select concept_id INTO @cd4_value from concept where uuid='3ceda710-26fe-102b-80cb-0017a47871b2';

  insert into obs(person_id, concept_id, encounter_id, obs_datetime, location_id, obs_group_id, value_numeric, creator, date_created, voided, uuid)
    select p.person_id, @cd4_value, e.encounter_id, r.obs_datetime, e.location_id, r.obs_id, r.value_numeric,1, now(), 0, uuid()
    from hivmigration_lab_results r join hivmigration_patients p on r.source_patient_id = p.source_patient_id
      join hivmigration_encounters e on r.source_encounter_id = e.source_encounter_id
    where r.value_numeric is not null and r.test_type ='cd4';

  -- Specimen Number
  select concept_id INTO @specimen_number from concept where uuid='162086AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA';

  insert into obs(person_id, concept_id, encounter_id, obs_datetime, location_id, obs_group_id, value_text, creator, date_created, voided, uuid)
    select p.person_id, @specimen_number, e.encounter_id, r.obs_datetime, e.location_id, r.obs_id, r.sample_id,1, now(), 0,  uuid()
    from hivmigration_lab_results r join hivmigration_patients p on r.source_patient_id = p.source_patient_id
      join hivmigration_encounters e on r.source_encounter_id = e.source_encounter_id where r.test_type ='hematocrit';


  -- hematocrit Count Numeric Value
  select concept_id INTO @hematocrit_value from concept where uuid='3cd69a98-26fe-102b-80cb-0017a47871b2';

  insert into obs(person_id, concept_id, encounter_id, obs_datetime, location_id, obs_group_id, value_numeric, creator, date_created, voided, uuid)
    select p.person_id, @hematocrit_value, e.encounter_id, r.obs_datetime, e.location_id, r.obs_id, r.value_numeric,1, now(), 0, uuid()
    from hivmigration_lab_results r join hivmigration_patients p on r.source_patient_id = p.source_patient_id
      join hivmigration_encounters e on r.source_encounter_id = e.source_encounter_id
    where r.value_numeric is not null and r.test_type ='hematocrit';

  -- Specimen Number
  select concept_id INTO @specimen_number from concept where uuid='162086AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA';

  insert into obs(person_id, concept_id, encounter_id, obs_datetime, location_id, obs_group_id, value_text, creator, date_created, voided, uuid)
    select p.person_id, @specimen_number, e.encounter_id, r.obs_datetime, e.location_id, r.obs_id, r.sample_id,1, now(), 0,  uuid()
    from hivmigration_lab_results r join hivmigration_patients p on r.source_patient_id = p.source_patient_id
      join hivmigration_encounters e on r.source_encounter_id = e.source_encounter_id where r.test_type ='ppd';


  -- ppd Count Numeric Value
  select concept_id INTO @ppd_value from concept where uuid='3cecf388-26fe-102b-80cb-0017a47871b2';

  insert into obs(person_id, concept_id, encounter_id, obs_datetime, location_id, obs_group_id, value_numeric, creator, date_created, voided, uuid)
    select p.person_id, @ppd_value, e.encounter_id, r.obs_datetime, e.location_id, r.obs_id, r.value_numeric,1, now(), 0, uuid()
    from hivmigration_lab_results r join hivmigration_patients p on r.source_patient_id = p.source_patient_id
      join hivmigration_encounters e on r.source_encounter_id = e.source_encounter_id
    where r.value_numeric is not null and r.test_type ='ppd';

  -- Specimen Number
  select concept_id INTO @specimen_number from concept where uuid='162086AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA';

  insert into obs(person_id, concept_id, encounter_id, obs_datetime, location_id, obs_group_id, value_text, creator, date_created, voided, uuid)
    select p.person_id, @specimen_number, e.encounter_id, r.obs_datetime, e.location_id, r.obs_id, r.sample_id,1, now(), 0,  uuid()
    from hivmigration_lab_results r join hivmigration_patients p on r.source_patient_id = p.source_patient_id
      join hivmigration_encounters e on r.source_encounter_id = e.source_encounter_id where r.test_type ='tr';

  -- Test rapid coded answer
  select concept_id INTO @test_rapid_value from concept where uuid='3cd6c946-26fe-102b-80cb-0017a47871b2';
  select concept_id INTO @positive from concept where uuid='3cd3a7a2-26fe-102b-80cb-0017a47871b2';
  select concept_id INTO @negative from concept where uuid='3cd28732-26fe-102b-80cb-0017a47871b2';

  insert into obs(person_id, concept_id, encounter_id, obs_datetime, location_id, obs_group_id, value_coded, creator, date_created, voided, uuid)
    select p.person_id, @test_rapid_value, e.encounter_id, r.obs_datetime, e.location_id, r.obs_id,
      case when ( r.value_boolean=1) then (@positive)
      else @negative
      end as value_coded
      ,1, now(), 0, uuid()
    from hivmigration_lab_results r join hivmigration_patients p on r.source_patient_id = p.source_patient_id
      join hivmigration_encounters e on r.source_encounter_id = e.source_encounter_id
    where r.test_type ='tr';

END