/*
 The only way I could get this to run was to do this in a very
 stepwise, almost brute-force method, but I think it works
 and it is easy to understand

 The approach is:
 	1) create a temp table with all pairs of encounters of the same
 		patient, date and encounter type
 	2) create another temp table of every obs of encounter_1 from the table of dups
 	4) go through that table and flag every obs from encounter_1 that is duplicated in encounter_2
 	5) select the pairs of encounters from the duplicate table with ONLY duplicate obs in encounter 2
 	6) write these to the final list to delete as such:
 		a) for pairs of encounters that are exact duplicates of each other, deduplicate and write to final list
 		b) for encounters that are subset of another, write directly to the final list


*/

select encounter_type_id into @drugdoc from encounter_type
where uuid = '0b242b71-5b60-11eb-8f5a-0242ac110002' ;

-- load a temp table with all pairs of encounters of the same
--  		patient, date and encounter type

drop temporary table if exists dup_pairs;
create temporary table dup_pairs
select e.encounter_id "encounter_id_1", e2.encounter_id "encounter_id_2"
from encounter e
         inner join encounter e2 on e2.voided = 0
    and e2.patient_id = e.patient_id
    and date(e.encounter_datetime) = date(e2.encounter_datetime)
    and e.encounter_type = e2.encounter_type
    and e.encounter_id <> e2.encounter_id
where e.voided = 0
  and e.encounter_type not in (@drugdoc)
limit 100 -- <<<<CHANGE!!!
;



-- create a temp table with the obs of encounter_id_1
drop temporary table if exists e1_obs;
CREATE TEMPORARY TABLE e1_obs
(
    encounter_id_1                      int(11),
    encounter_id_2                      int(11),
    obs_id								int(11),
    concept_id							int(11),
    value_coded							int(11),
    value_drug							int(11),
    value_datetime						datetime,
    value_numeric						double,
    value_text							text,
    dup									int(1)
);

insert into e1_obs (encounter_id_1, encounter_id_2, obs_id,concept_id, value_coded, value_drug,value_datetime,value_numeric,value_text)
select d.encounter_id_1, d.encounter_id_2, obs_id,concept_id, value_coded, value_drug,value_datetime,value_numeric,value_text
from dup_pairs d
         inner join obs o on o.encounter_id  = d.encounter_id_1
    and o.voided = 0
;


CREATE INDEX e1_obs_i1 ON e1_obs (encounter_id_1);
CREATE INDEX e1_obs_i2 ON e1_obs (concept_id);

-- flag those obs that are contained in encounter_id_2
update e1_obs o1
set dup = 1
where EXISTS
          (select 1 from obs o2
           where o2.voided = 0
             and o2.encounter_id = o1.encounter_id_2
             and o1.concept_id = o2.concept_id
             and (o1.value_coded = o2.value_coded
               or (o1.value_coded is null and o2.value_coded  is null))
             and (o1.value_text = o2.value_text
               or (o1.value_text is null and o2.value_text  is null))
             and (o1.value_numeric = o2.value_numeric
               or (o1.value_numeric is null and o2.value_numeric  is null))
-- 	and (o1.value_drug = o2.value_drug
-- 		or (o1.value_drug is null and o2.value_drug  is null))
             and (o1.value_datetime= o2.value_datetime
               or (o1.value_datetime is null and o2.value_datetime  is null))
          );

-- load a table with only encounters containing ONLY duplicate obs
drop temporary table if exists dup_obs;
create temporary table dup_obs
select d.encounter_id_1 "encounter_id_1",
       d.encounter_id_2 "encounter_id_2"
from dup_pairs d
         inner join encounter e on e.encounter_id  = d.encounter_id_1
where not exists
    (select 1 from e1_obs e1
     where e1.encounter_id_1 = d.encounter_id_1
       and e1.encounter_id_2 = d.encounter_id_2
       and e1.dup is null)
order by 1,2
;

-- replicating dup_obs table for use in next query
drop temporary table if exists dup_obs_2;
create temporary table dup_obs_2
select * from dup_obs;

CREATE INDEX dup_obs_e1 ON dup_obs (encounter_id_1);
CREATE INDEX dup_obs_e2 ON dup_obs (encounter_id_2);
CREATE INDEX dup_obs2_e1 ON dup_obs_2 (encounter_id_1);
CREATE INDEX dup_obs2_e2 ON dup_obs_2 (encounter_id_2);

-- load a temp table with those encounters that are exact duplicates of each other
drop temporary table if exists exact_dups;
create temporary table exact_dups
select * from dup_obs d1
where exists
          (select 1 from dup_obs_2 d2
           where d1.encounter_id_1 = d2.encounter_id_2
             and d1.encounter_id_2 = d2.encounter_id_1)
;

CREATE INDEX exact_dups_e1 ON exact_dups (encounter_id_1);

-- inserting into the final temp table with encounters to delete out of the set of exact duplicates
-- note these are deduplicated here so both duplicates won't be voided
drop temporary table if exists enc_to_delete;
create temporary table enc_to_delete
select distinct
    if(encounter_id_1 < encounter_id_2, encounter_id_1,encounter_id_2) "encounter_id"
from exact_dups
;

-- insert into final encounters to delete those encounters that are NOT exact duplicates
-- (i.e. those that are a subset of another encounter)
insert into enc_to_delete (encounter_id)
select encounter_id_1 from dup_obs d1
where not exists
    (select 1 from dup_obs_2 d2
     where d1.encounter_id_1 = d2.encounter_id_2
       and d1.encounter_id_2 = d2.encounter_id_1)
;


-- void based on that final list:
select * from enc_to_delete ;
/*
update encounter e
inner join enc_to_delete ed on ed.encounter_id = e.encounter_id
set e.voided = 1,
	e.void_reason =   'duplicate encounters due to system error'
;

update obs o
inner join enc_to_delete ed on ed.encounter_id = o.encounter_id
set o.voided = 1,
	o.void_reason =  'duplicate encounters due to system error'

*/