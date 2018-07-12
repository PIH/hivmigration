create procedure load_vital_signs()
begin
  declare weight_concept int;
  declare height_concept int;
  declare bmi_concept int;

  select concept_id into weight_concept from concept
  where uuid = '3ce93b62-26fe-102b-80cb-0017a47871b2';

  select concept_id into height_concept from concept
  where uuid = '3ce93cf2-26fe-102b-80cb-0017a47871b2';

  select concept_id into bmi_concept from concept
  where uuid = '3ce14da8-26fe-102b-80cb-0017a47871b2';

  -- height (cm)
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
     value_numeric
  )
  select p.person_id,
    height_concept,
    e.encounter_id,
    e.encounter_date,
    e.location_id,
    1,
    now(),
    0,
    v.vital_sign_uuid,
    v.result
  from hivmigration_vital_signs v
  join hivmigration_encounters e on v.source_encounter_id = e.source_encounter_id
  join hivmigration_patients p on e.source_patient_id = p.source_patient_id
  where v.sign = 'height'
  and v.result_unit = 'cm';

  -- weight (kgs)
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
     value_numeric
  )
  select p.person_id,
    weight_concept,
    e.encounter_id,
    e.location_id,
    1,
    now(),
    e.encounter_date,
    0,
    v.vital_sign_uuid,
    case
      when v.result_unit = 'lbs' then v.result * 0.453592
      else v.result
    end
  from hivmigration_vital_signs v
  join hivmigration_encounters e on v.source_encounter_id = e.source_encounter_id
  join hivmigration_patients p on e.source_patient_id = p.source_patient_id
  where v.sign = 'weight'
  and v.result_unit in ('kgs', 'lbs');

  -- bmi (kg/m2)
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
     value_numeric
  )
  select p.person_id,
    bmi_concept,
    e.encounter_id,
    e.encounter_date,
    e.location_id,
    1,
    now(),
    0,
    v.vital_sign_uuid,
    v.result
  from hivmigration_vital_signs v
  join hivmigration_encounters e on v.source_encounter_id = e.source_encounter_id
  join hivmigration_patients p on e.source_patient_id = p.source_patient_id
  where v.sign = 'bmi'
  and v.result_unit = 'kg_per_m2';
end;
