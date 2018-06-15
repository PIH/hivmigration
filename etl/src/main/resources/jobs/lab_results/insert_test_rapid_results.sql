CREATE PROCEDURE insert_test_rapid_results ()
  BEGIN

    DECLARE test_rapid_construct int;
    DECLARE specimen_number int;
    DECLARE positive int;
    DECLARE negative int;


    -- test_rapid Construct
    select concept_id INTO @test_rapid_construct from concept where uuid='A19F5A83-E960-413D-B93B-9270C53580A2';

    -- add OBS_GROUP  test_rapid Construct
    insert into obs(obs_id, person_id, concept_id, encounter_id, obs_datetime, location_id, creator, date_created, voided, uuid)
      select r.obs_id, p.person_id, @test_rapid_construct, e.encounter_id, r.obs_datetime, e.location_id, 1, now(), 0, r.uuid
      from hivmigration_lab_results r join hivmigration_patients p on r.source_patient_id = p.source_patient_id
        join hivmigration_encounters e on r.source_encounter_id = e.source_encounter_id where r.test_type ='tr';

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
