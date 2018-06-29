create procedure load_medpickup_date_of_next_visit()
begin
  declare date_of_next_visit_concept int;

  select concept_id into date_of_next_visit_concept from concept
  where uuid = '8266b6fb-7a49-11e8-8624-54ee75ef41c2';

  insert ignore into obs(
     person_id,
     concept_id,
     encounter_id,
     obs_datetime,
     location_id,
     creator,
     date_created,
     voided,
     uuid,
     value_datetime
  )
  select p.person_id,
    date_of_next_visit_concept,
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
    date(o.value)
  from hivmigration_observations o
  join hivmigration_encounters e on o.source_encounter_id = e.source_encounter_id
  join hivmigration_patients p on e.source_patient_id = p.source_patient_id
  where o.observation = 'accompagnateur_date_of_next_visit'
        and e.source_encounter_type = 'accompagnateur';
end;
