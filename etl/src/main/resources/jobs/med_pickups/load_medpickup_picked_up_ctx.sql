create procedure load_medpickup_picked_up_ctx()
begin
  declare picked_up_ctx_concept int;
  declare yes_concept int;
  declare no_concept int;

  select concept_id into picked_up_ctx_concept from concept
  where uuid = '8b0472fc-7a49-11e8-8624-54ee75ef41c2';

  select concept_id into yes_concept from concept
  where uuid = '3cd6f600-26fe-102b-80cb-0017a47871b2';

  select concept_id into no_concept from concept
  where uuid = '3cd6f86c-26fe-102b-80cb-0017a47871b2';

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
     value_coded
  )
  select p.person_id,
    picked_up_ctx_concept,
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
    case o.value
      when 't' then yes_concept
      else no_concept
    end
  from hivmigration_observations o
  join hivmigration_encounters e on o.source_encounter_id = e.source_encounter_id
  join hivmigration_patients p on e.source_patient_id = p.source_patient_id
  where o.observation = 'accompagnateur_picked_up_ctx'
        and e.source_encounter_type = 'accompagnateur';
end;
