create procedure load_exam_extra()
begin
  declare return_visit_date_concept int;

  select concept_id into return_visit_date_concept from concept
  where uuid = '3ce94df0-26fe-102b-80cb-0017a47871b2';

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
     value_datetime
  )
  select p.person_id,
    return_visit_date_concept,
    e.encounter_id,
    e.encounter_date,
    e.location_id,
    1,
    now(),
    0,
    v.exam_extra_uuid,
    v.next_exam_date
  from hivmigration_exam_extra v
  join hivmigration_encounters e on v.source_encounter_id = e.source_encounter_id
  join hivmigration_patients p on e.source_patient_id = p.source_patient_id
  where v.next_exam_date is not null
  and DATEDIFF(v.next_exam_date, now()) > 0
  and DATEDIFF(v.next_exam_date, now()) < 3650; -- fetch appts in the next 10 years

end;
