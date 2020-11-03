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
            INSERT INTO hivmigration_data_warnings (openmrs_patient_id, openmrs_encounter_id, encounter_date, field_name, field_value, warning_type, warning_details) values(?,?,?,?,?,?,?) 
            ''', '''
                select  e.patient_id as patient_id,
                        e.encounter_id as encounter_id,
                        e.encounter_date as encounter_date,
                        'next_exam_date' as field_name,
                        to_char(x.next_exam_date, 'yyyy-mm-dd') as field_value,
                        'next_exam_date found on encounter other than intake or followup',
                        CONCAT('Encounter type: ', e.type)
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
            from hiv_observations o, hiv_encounters e, hiv_demographics_real d, hiv_institutions i  
            where o.ENCOUNTER_ID = e.ENCOUNTER_ID and e.patient_id = d.patient_id and o.VALUE = i.INSTITUTION_ID 
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

        create_tmp_obs_table()

        executeMysql("Create staging table for migrating TB screening questions", '''
            create table hivmigration_tb_screening (                            
              source_encounter_id int,
              fever_night_sweat BOOLEAN,
              fever_night_sweat_comments VARCHAR(255),
              weight_loss BOOLEAN,
              weight_loss_comments VARCHAR(255),
              cough BOOLEAN,
              cough_comments VARCHAR(255),              
              tb_contact BOOLEAN,              
              bloodyCough BOOLEAN,
              bloodyCough_comments VARCHAR(255),
              dyspnea BOOLEAN,
              dyspnea_comments VARCHAR(255),
              chestPain BOOLEAN, 
              chestPain_comments VARCHAR(255)                     
            );
        ''')

        // migrate TB Screening questions
        loadFromOracleToMySql('''
            insert into hivmigration_tb_screening (
              source_encounter_id,
              fever_night_sweat,
              fever_night_sweat_comments,
              weight_loss,
              weight_loss_comments,
              cough,
              cough_comments,              
              tb_contact,
              bloodyCough,
              bloodyCough_comments,
              dyspnea,
              dyspnea_comments,
              chestPain, 
              chestPain_comments
            )
            values(?,?,?,?,?,?,?,?,?,?,?,?,?,?) 
            ''', '''
                select      e.encounter_id,
                            case 
                                when ((fever.result = 't') or (ntsweat.result='t')) then 1 
                                when ((fever.result is null) and (ntsweat.result is null)) then null 
                                else 0 
                            end as fever_night_sweat,                   
                            'fever_result=' || fever.result || ',fever_duration=' || fever.duration || ',fever_duration_unit=' || fever.duration_unit 
                                    || '; night_sweat_result=' || ntsweat.result || ',night_sweat_duration=' || ntsweat.duration || ',night_sweat_duration_unit=' || ntsweat.duration_unit as fever_comments
                            ,                                
                            decode(wtloss.result, 't', 1, 'f', 0, null) as weight_loss, 
                            'weight_loss_duration=' || wtloss.duration || ',weight_loss_duration_unit=' || wtloss.duration_unit as weight_loss_comments,                               
                            decode(cough.result, 't', 1, 'f', 0, null) as cough, 
                            'cough_duration=' || cough.duration || ',cough_duration_unit=' || cough.duration_unit as cough_comments,                              
                            decode(tbcontact.result, 't', 1, 'f', 0, null) as tb_contact, 
                            decode(bloodyCough.result, 't', 1, 'f', 0, null) as bloodyCough, 
                            'bloodyCough_duration=' || bloodyCough.duration || ',bloodyCough_duration_unit=' || bloodyCough.duration_unit as bloodyCough_comments,
                            decode(dyspnea.result, 't', 1, 'f', 0, null) as dyspnea_result, 
                            'dyspnea_duration=' || dyspnea.duration || ',dyspnea_duration_unit=' || dyspnea.duration_unit as dyspnea_comments, 
                            decode(chestPain.result, 't', 1, 'f', 0, null) as chestPain_result, 
                            'chestPain_duration=' || chestPain.duration || ',chestPain_duration_unit=' || chestPain.duration_unit as chestPain_comments
                from        hiv_encounters e 
                join hiv_demographics_real d on e.patient_id = d.patient_id 
                left join   (select encounter_id, symptom, result, duration, duration_unit from hiv_exam_symptoms) cough on cough.ENCOUNTER_ID = e.ENCOUNTER_ID and cough.SYMPTOM = 'cough\'
                left join   (select encounter_id, symptom, result, duration, duration_unit from hiv_exam_symptoms) fever on fever.ENCOUNTER_ID = e.ENCOUNTER_ID and fever.SYMPTOM = 'fever\'
                left join   (select encounter_id, symptom, result, duration, duration_unit from hiv_exam_symptoms) wtloss on wtloss.ENCOUNTER_ID = e.ENCOUNTER_ID and wtloss.SYMPTOM = 'loss_of_weight\'
                left join   (select encounter_id, symptom, result, duration, duration_unit from hiv_exam_symptoms) ntsweat on ntsweat.ENCOUNTER_ID = e.ENCOUNTER_ID and ntsweat.SYMPTOM = 'night_sweats\'
                left join   (select encounter_id, symptom, result, duration, duration_unit from hiv_exam_symptoms) tbcontact on tbcontact.ENCOUNTER_ID = e.ENCOUNTER_ID and tbcontact.SYMPTOM = 'tb_contact\'
                left join   (select encounter_id, symptom, result, duration, duration_unit from hiv_exam_symptoms) bloodyCough on bloodyCough.ENCOUNTER_ID = e.ENCOUNTER_ID and bloodyCough.SYMPTOM = 'hymoptusis\'
                left join   (select encounter_id, symptom, result, duration, duration_unit from hiv_exam_symptoms) dyspnea on dyspnea.ENCOUNTER_ID = e.ENCOUNTER_ID and dyspnea.SYMPTOM = 'dyspnea' 
                left join   (select encounter_id, symptom, result, duration, duration_unit from hiv_exam_symptoms) chestPain on chestPain.ENCOUNTER_ID = e.ENCOUNTER_ID and chestPain.SYMPTOM = 'chest_pain\'
                where       ( cough.result is not null or fever.result is not null or wtloss.result is not null 
                            or ntsweat.result is not null 
                            or tbcontact.result is not null 
                            or bloodyCough.result is not null 
                            or dyspnea.result is not null 
                            or chestPain.result is not null);
            ''')

        executeMysql("Load TB screening questions as observations", ''' 
                     
            -- Fever and night sweats                                            
            INSERT INTO tmp_obs (value_coded_uuid, source_encounter_id, concept_uuid, comments)
            SELECT 
                  concept_uuid_from_mapping('PIH', '11565')
                  , source_encounter_id
                  , IF(fever_night_sweat=1, concept_uuid_from_mapping('PIH', '11563'), concept_uuid_from_mapping('PIH', '11564'))
                  , fever_night_sweat_comments
            FROM hivmigration_tb_screening
            WHERE fever_night_sweat = 1 or fever_night_sweat = 0;  
            
            -- Weight loss                                            
            INSERT INTO tmp_obs (value_coded_uuid, source_encounter_id, concept_uuid, comments)
            SELECT 
                  concept_uuid_from_mapping('PIH', '11566')
                  , source_encounter_id
                  , IF(weight_loss=1, concept_uuid_from_mapping('PIH', '11563'), concept_uuid_from_mapping('PIH', '11564'))
                  , weight_loss_comments
            FROM hivmigration_tb_screening
            WHERE weight_loss = 1 or weight_loss = 0; 
            
            -- Cough                                            
            INSERT INTO tmp_obs (value_coded_uuid, source_encounter_id, concept_uuid, comments)
            SELECT 
                  concept_uuid_from_mapping('PIH', '11567')
                  , source_encounter_id
                  , IF(cough=1, concept_uuid_from_mapping('PIH', '11563'), concept_uuid_from_mapping('PIH', '11564'))
                  , cough_comments
            FROM hivmigration_tb_screening
            WHERE cough = 1 or cough = 0; 
            
            -- TB contact                                            
            INSERT INTO tmp_obs (value_coded_uuid, source_encounter_id, concept_uuid)
            SELECT 
                  concept_uuid_from_mapping('PIH', '11568')
                  , source_encounter_id
                  , IF(tb_contact=1, concept_uuid_from_mapping('PIH', '11563'), concept_uuid_from_mapping('PIH', '11564'))                  
            FROM hivmigration_tb_screening
            WHERE tb_contact = 1 or tb_contact = 0;   
            
            -- Bloody Cough(Hymoptusis)                                            
            INSERT INTO tmp_obs (value_coded_uuid, source_encounter_id, concept_uuid, comments)
            SELECT 
                  concept_uuid_from_mapping('PIH', '970')
                  , source_encounter_id
                  , IF(bloodyCough=1, concept_uuid_from_mapping('PIH', '11563'), concept_uuid_from_mapping('PIH', '11564'))
                  , bloodyCough_comments
            FROM hivmigration_tb_screening
            WHERE bloodyCough = 1 or bloodyCough = 0;       
            
            -- Difficulty breathing(Dyspnea)                                            
            INSERT INTO tmp_obs (value_coded_uuid, source_encounter_id, concept_uuid, comments)
            SELECT 
                  concept_uuid_from_mapping('PIH', '5960')
                  , source_encounter_id
                  , IF(dyspnea=1, concept_uuid_from_mapping('PIH', '11563'), concept_uuid_from_mapping('PIH', '11564'))
                  , dyspnea_comments
            FROM hivmigration_tb_screening
            WHERE dyspnea = 1 or dyspnea = 0;     
            
            -- Chest Pain                                            
            INSERT INTO tmp_obs (value_coded_uuid, source_encounter_id, concept_uuid, comments)
            SELECT 
                  concept_uuid_from_mapping('PIH', '136')
                  , source_encounter_id
                  , IF(chestPain=1, concept_uuid_from_mapping('PIH', '11563'), concept_uuid_from_mapping('PIH', '11564'))
                  , chestPain_comments
            FROM hivmigration_tb_screening
            WHERE chestPain = 1 or chestPain = 0;  
            
         
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
                        where  e.encounter_type=t.encounter_type_id and t.uuid='cc1720c9-3e4c-4fa8-a7ec-40eeaad1958c'
                    );          
        ''')
        executeMysql("drop table if exists hivmigration_exam_extra")
        executeMysql("drop table if exists hivmigration_transfer_out_to")
        executeMysql("drop table if exists hivmigration_tb_screening")
    }
}
