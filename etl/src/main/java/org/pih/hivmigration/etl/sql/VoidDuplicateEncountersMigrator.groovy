package org.pih.hivmigration.etl.sql

/**
The only way I could get this to run was to do this in a very stepwise, almost brute-force method, but I think it works
and it is easy to understand.  The approach is:
    1) create a temp table with all pairs of encounters of the same patient, date and encounter type
    2) create another temp table of every obs of encounter_1 from the table of dups
    4) go through that table and flag every obs from encounter_1 that is duplicated in encounter_2
    5) select the pairs of encounters from the duplicate table with ONLY duplicate obs in encounter 2
    6) write these to the final list to delete as such:
        a) for pairs of encounters that are exact duplicates of each other, deduplicate and write to final list
        b) for encounters that are subset of another, write directly to the final list
*/
class VoidDuplicateEncountersMigrator extends SqlMigrator {

    @Override
    void migrate() {

        executeMysql("Load a temp table with all pairs of encounters of the same patient, date and encounter type ", '''
            SET @drugdoc = (SELECT encounter_type_id FROM encounter_type WHERE uuid='0b242b71-5b60-11eb-8f5a-0242ac110002'); 
            drop table if exists hivmigration_dup_pairs;
            create table hivmigration_dup_pairs 
                select e.encounter_id as encounter_id_1, e2.encounter_id as encounter_id_2
                from encounter e
                inner join encounter e2 on e2.voided = 0 
                and e2.patient_id = e.patient_id 
                and date(e.encounter_datetime) = date(e2.encounter_datetime)
                and e.encounter_type = e2.encounter_type 
                and e.encounter_id <> e2.encounter_id 
                where e.voided = 0 
                and e.encounter_type not in (@drugdoc) 
            ;
        ''');

        executeMysql("create a temp table with the obs of encounter_id_1", '''
            drop table if exists hivmigration_temp_e1_obs;
            CREATE TABLE hivmigration_temp_e1_obs
            (
                encounter_id_1 int(11),
                encounter_id_2 int(11),
                obs_id int(11),
                concept_id int(11),
                value_coded int(11),
                value_drug int(11),
                value_datetime datetime,
                value_numeric double,
                value_text text,    
                dup int(1)
            );
            insert into hivmigration_temp_e1_obs (encounter_id_1, encounter_id_2, obs_id,concept_id, value_coded, value_drug,value_datetime,value_numeric,value_text)
                select d.encounter_id_1, d.encounter_id_2, obs_id,concept_id, value_coded, value_drug,value_datetime,value_numeric,value_text
                from hivmigration_dup_pairs d
                inner join obs o on o.encounter_id  = d.encounter_id_1
                and o.voided = 0
            ;
            CREATE INDEX hivmigration_temp_e1_obs_i1 ON hivmigration_temp_e1_obs (encounter_id_1);
            CREATE INDEX hivmigration_temp_e1_obs_i2 ON hivmigration_temp_e1_obs (concept_id);
            CREATE INDEX hivmigration_temp_e1_obs_i3 ON hivmigration_temp_e1_obs (encounter_id_2);
        ''');

        executeMysql("flag those obs that are contained in encounter_id_2", '''
            update hivmigration_temp_e1_obs o1
            set dup = 1 
            where EXISTS (
                select 1 from obs o2
                where o2.voided = 0
                and o2.encounter_id = o1.encounter_id_2
                and o1.concept_id = o2.concept_id
                and (o1.value_coded = o2.value_coded or (o1.value_coded is null and o2.value_coded  is null))
                and (o1.value_text = o2.value_text or (o1.value_text is null and o2.value_text  is null))
                and (o1.value_numeric = o2.value_numeric or (o1.value_numeric is null and o2.value_numeric  is null))
                and (o1.value_datetime= o2.value_datetime or (o1.value_datetime is null and o2.value_datetime  is null))
            );
        ''');

        executeMysql("load a table with only encounters containing ONLY duplicate obs", '''
            drop table if exists hivmigration_dup_obs;
            create table hivmigration_dup_obs
                select d.encounter_id_1 as encounter_id_1,
                d.encounter_id_2 as encounter_id_2
                from hivmigration_dup_pairs d
                inner join encounter e on e.encounter_id  = d.encounter_id_1
                where not exists (
                    select 1 from hivmigration_temp_e1_obs e1
                    where e1.encounter_id_1 = d.encounter_id_1
                    and e1.encounter_id_2 = d.encounter_id_2
                    and e1.dup is null
                )
                order by 1,2
            ;
        ''');

        executeMysql("replicating hivmigration_dup_obs table for use in next query", '''
            drop table if exists hivmigration_dup_obs_2;
            create table hivmigration_dup_obs_2
                select * from hivmigration_dup_obs;
            
            CREATE INDEX hivmigration_dup_obs_e1 ON hivmigration_dup_obs (encounter_id_1);
            CREATE INDEX hivmigration_dup_obs_e2 ON hivmigration_dup_obs (encounter_id_2);
            CREATE INDEX hivmigration_dup_obs2_e1 ON hivmigration_dup_obs_2 (encounter_id_1);
            CREATE INDEX hivmigration_dup_obs2_e2 ON hivmigration_dup_obs_2 (encounter_id_2);

        ''');

        executeMysql("load a temp table with those encounters that are exact duplicates of each other", '''
            drop table if exists hivmigration_exact_dups;
            create table hivmigration_exact_dups
                select * from hivmigration_dup_obs d1
                where exists (
                    select 1 from hivmigration_dup_obs_2 d2
                    where d1.encounter_id_1 = d2.encounter_id_2
                    and d1.encounter_id_2 = d2.encounter_id_1
                )
            ;
            CREATE INDEX hivmigration_exact_dups_e1 ON hivmigration_exact_dups (encounter_id_1);
        ''');

        executeMysql("inserting into the final temp table with encounters to delete out of the set of exact duplicates", '''
            drop table if exists hivmigration_enc_to_delete;
            create table hivmigration_enc_to_delete
                select distinct if(encounter_id_1 < encounter_id_2, encounter_id_1,encounter_id_2) as encounter_id
                from hivmigration_exact_dups
            ;
            insert into hivmigration_enc_to_delete (encounter_id)
            select encounter_id_1 from hivmigration_dup_obs d1
            where not exists (
                select 1 from hivmigration_dup_obs_2 d2 where d1.encounter_id_1 = d2.encounter_id_2 and d1.encounter_id_2 = d2.encounter_id_1
            );
        ''');

        executeMysql("void encounters based on that final list", '''
            update encounter e 
            inner join hivmigration_enc_to_delete ed on ed.encounter_id = e.encounter_id 
            set e.voided = 1, e.void_reason = 'duplicate encounters due to system error'
            ;
        ''');

        executeMysql("void obs based on that final list", '''
            update obs o 
            inner join hivmigration_enc_to_delete ed on ed.encounter_id = o.encounter_id 
            set o.voided = 1, o.void_reason = 'duplicate encounters due to system error';
        ''')
    }

    @Override
    void revert() {
        executeMysql("unvoid obs based on that final list", '''
            update obs o
            inner join hivmigration_enc_to_delete ed on ed.encounter_id = o.encounter_id
            set o.voided = 0, o.void_reason = null
            where o.voided = 1 and o.void_reason = 'duplicate encounters due to system error'; 
        ''')

        executeMysql("unvoid encounters based on that final list", '''
            update encounter e 
            inner join hivmigration_enc_to_delete ed on ed.encounter_id = e.encounter_id 
            set e.voided = 0, e.void_reason = null
            where e.voided = 1 and e.void_reason = 'duplicate encounters due to system error'
        ''')

        executeMysql("drop table if exists hivmigration_enc_to_delete;")
        executeMysql("drop table if exists hivmigration_exact_dups;")
        executeMysql("drop table if exists hivmigration_dup_obs_2;")
        executeMysql("drop table if exists hivmigration_dup_obs;")
        executeMysql("drop table if exists hivmigration_temp_e1_obs;")
        executeMysql("drop table if exists hivmigration_dup_pairs;")

    }

}
