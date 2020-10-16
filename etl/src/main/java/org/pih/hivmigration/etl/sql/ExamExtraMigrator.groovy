package org.pih.hivmigration.etl.sql

class ExamExtraMigrator extends ObsMigrator {

    @Override
    def void migrate() {

        executeMysql("Create staging table for migrating next scheduled visit date", '''
            create table hivmigration_exam_extra (                            
              source_encounter_id int,
              source_patient_id int,
              next_exam_date date            
            );
        ''')

        // note any next_exam_dates not on intake and followup form
        loadFromOracleToMySql('''
            INSERT INTO hivmigration_data_warnings (patient_id, encounter_id, encounter_date, field_name, field_value, note) values(?,?,?,?,?,?) 
            ''', '''
                select  e.patient_id as patient_id,
                        e.encounter_id as encounter_id,
                        e.encounter_date as encounter_date,
                        'next_exam_date' as field_name,
                        to_char(x.next_exam_date, 'yyyy-mm-dd') as field_value,
                        CONCAT('next_exam_date found on encounter of type ', e.type)
                from hiv_exam_extra x,hiv_encounters e, hiv_demographics_real d 
                where x.next_exam_date is not null and x.encounter_id = e.encounter_id and e.patient_id = d.patient_id
                    and e.type not in ('intake','followup')
            ''')


        // migrate next_exam_dates on intake and followup forms
        loadFromOracleToMySql('''
            insert into hivmigration_exam_extra (
              source_encounter_id,
              source_patient_id,
              next_exam_date 
            )
            values(?,?,?) 
            ''', '''
                select x.encounter_id as source_encounter_id,
                        e.patient_id as source_patient_id,
                        to_char(x.next_exam_date, 'yyyy-mm-dd') as next_exam_date
                from hiv_exam_extra x,hiv_encounters e, hiv_demographics_real d 
                where x.next_exam_date is not null and x.encounter_id = e.encounter_id and e.patient_id = d.patient_id
                    and e.type in ('intake','followup')
            ''')

        create_tmp_obs_table()

        executeMysql("Load next visit date as observations", ''' 
                      
            SET @next_visit_date_concept_uuid = '3ce94df0-26fe-102b-80cb-0017a47871b2';            
                        
            INSERT INTO tmp_obs (value_datetime, source_patient_id, source_encounter_id, concept_uuid)
            SELECT next_exam_date, source_patient_id, source_encounter_id, @next_visit_date_concept_uuid
            FROM hivmigration_exam_extra
            WHERE next_exam_date is not null;            
        ''')

        migrate_tmp_obs()

        executeMysql("Create staging table for migrating transfer_out_to obs", '''
            create table hivmigration_transfer_out_to (                            
              source_encounter_id int PRIMARY KEY,
              source_patient_id int,                            
              transfer_out_to VARCHAR(48) ,               
              obs_date date             
            );
        ''')

        loadFromOracleToMySql('''
            insert into hivmigration_transfer_out_to
                (source_encounter_id,
                 source_patient_id,
                 transfer_out_to,
                 obs_date)
            values (?, ?, ?, ?)
            ''', '''
            select o.encounter_id as source_encounter_id,
                    e.patient_id as source_patient_id,         
                    i.name as transfer_out_to,
                    to_char(o.entry_date, 'yyyy-mm-dd') as obs_date 
            from hiv_observations o, hiv_encounters e, hiv_institutions i  
            where o.ENCOUNTER_ID = e.ENCOUNTER_ID and o.VALUE = i.INSTITUTION_ID 
                    and observation='transfer_out_to' and o.value is not null 
            order by o.ENCOUNTER_ID 
        ''')

        create_tmp_obs_table()

        executeMysql("Load transferred_out observations", ''' 
                      
            SET @disposition_category = 'c8b22b09-e2f2-4606-af7d-e52579996de3'; 
            SET @transferred_out = '3cdd5c02-26fe-102b-80cb-0017a47871b2';            
                        
            INSERT INTO tmp_obs (value_coded_uuid, source_patient_id, source_encounter_id, concept_uuid)
            SELECT @transferred_out, source_patient_id, source_encounter_id, @disposition_category
            FROM hivmigration_transfer_out_to
            WHERE transfer_out_to is not null;     
            
            -- External transfers
            SET @transferred_out_location = '113a5ce0-6487-4f45-964d-2dcbd7d23b67'; 
            SET @external_location = '5b1f137c-b757-46c3-9735-c2fcb6ba221d';
            
            INSERT INTO tmp_obs (value_coded_uuid, source_patient_id, source_encounter_id, concept_uuid)
            SELECT @external_location, source_patient_id, source_encounter_id, @transferred_out_location
            FROM hivmigration_transfer_out_to
            WHERE transfer_out_to = 'Autre (non-ZL)';
            
            -- Internal transfers
            SET @internal_location = '83192b97-480c-4533-8ede-aeac15a3a736';
            
            INSERT INTO tmp_obs (value_coded_uuid, source_patient_id, source_encounter_id, concept_uuid)
            SELECT @internal_location, source_patient_id, source_encounter_id, @transferred_out_location
            FROM hivmigration_transfer_out_to
            WHERE transfer_out_to != 'Autre (non-ZL)';
            
            -- ZL transfered location name
            SET @zl_transfer_site = 'a96352e3-3afc-418b-b79f-3290fc26a3b3';
            
            INSERT INTO tmp_obs (value_text, source_patient_id, source_encounter_id, concept_uuid)
            SELECT transfer_out_to, source_patient_id, source_encounter_id, @zl_transfer_site
            FROM hivmigration_transfer_out_to
            WHERE transfer_out_to != 'Autre (non-ZL)';
                   
        ''')

        migrate_tmp_obs()
    }

    @Override
    def void revert() {
        // delete all Return Visit Dates except Next Dispensed Date (see Medpickups)
        executeMysql("Delete all Return Visit Date obs except the Next Dispense Date obs of the Dispensing encounters", '''
                delete from obs where concept_id in (select concept_id from concept where uuid='3ce94df0-26fe-102b-80cb-0017a47871b2')
                    and encounter_id not in( 
                        select e.encounter_id
                        from encounter e, encounter_type t 
                        where  e.encounter_type=t.encounter_type_id and t.uuid='cc1720c9-3e4c-4fa8-a7ec-40eeaad1958c\'
                    );          
        ''')
        executeMysql("drop table if exists hivmigration_exam_extra")
        executeMysql("drop table if exists hivmigration_transfer_out_to")
    }
}
