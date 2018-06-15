CREATE PROCEDURE insert_cd4_results ()
  BEGIN

    DECLARE cd4_construct int;
    DECLARE specimen_number int;
    DECLARE cd4_value int;

    -- CD4 Count Construct
    select concept_id INTO @cd4_construct from concept where uuid='37769FDB-5FC1-4D47-82C2-DB88960BB224';

    -- add OBS_GROUP  CD4 Count Construct
    insert into obs(obs_id, person_id, concept_id, encounter_id, obs_datetime, location_id, creator, date_created, voided, uuid)
      select r.obs_id, p.person_id, @cd4_construct, e.encounter_id, r.obs_datetime, e.location_id, 1, now(), 0, r.uuid
      from hivmigration_lab_results r join hivmigration_patients p on r.source_patient_id = p.source_patient_id
        join hivmigration_encounters e on r.source_encounter_id = e.source_encounter_id where r.test_type ='cd4';

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


  END
