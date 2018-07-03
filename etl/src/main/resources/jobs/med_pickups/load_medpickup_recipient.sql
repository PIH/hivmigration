create procedure load_medpickup_recipient()
begin
  declare chw_name_concept int;
  declare picked_up_by_patient_concept int;

  select concept_id into chw_name_concept from concept
  where uuid = '164141AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA';

  select concept_id into picked_up_by_patient_concept from concept
  where uuid = '6a38b794-7a49-11e8-8624-54ee75ef41c2';

  -- record accompagnateurs who picked up meds
  insert into obs(
     person_id,
     concept_id,
     encounter_id,
     obs_datetime,
     location_id,
     creator,
     date_created,
     voided,
     uuid,
     value_text
  )
  select p.person_id,
    chw_name_concept,
    e.encounter_id,
    case
      when o.entry_date is not null then o.entry_date
      else e.encounter_date
    end,
    e.location_id,
    1,
    now(),
    0,
    o.observation_uuid,
    o.value
  from hivmigration_observations o
  join hivmigration_encounters e on o.source_encounter_id = e.source_encounter_id
  join hivmigration_patients p on e.source_patient_id = p.source_patient_id
  where o.observation = 'accompagnateur_name'
        and e.source_encounter_type = 'accompagnateur'
        and lower(o.value) not in ('lui meme', 'elle meme', 'soi meme', 'soi-meme');
  -- other possible values to filter by: Auto-medication, Automedicatio, AUTOMEDICATION, Auto-Med, AutoMed

  -- record patients who picked up their own meds
  insert into obs(
    person_id,
    concept_id,
    encounter_id,
    obs_datetime,
    location_id,
    creator,
    date_created,
    voided,
    uuid,
    value_text
  )
    select p.person_id,
      picked_up_by_patient_concept,
      e.encounter_id,
      case
        when o.entry_date is not null then o.entry_date
        else e.encounter_date
      end,
      e.location_id,
      1,
      now(),
      0,
      o.observation_uuid,
      o.value
    from hivmigration_observations o
      join hivmigration_encounters e on o.source_encounter_id = e.source_encounter_id
      join hivmigration_patients p on e.source_patient_id = p.source_patient_id
    where o.observation = 'accompagnateur_name'
          and e.source_encounter_type = 'accompagnateur'
          and lower(o.value) in ('lui meme', 'elle meme', 'soi meme', 'soi-meme');
end;



