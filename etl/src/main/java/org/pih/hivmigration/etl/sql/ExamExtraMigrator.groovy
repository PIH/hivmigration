package org.pih.hivmigration.etl.sql

class ExamExtraMigrator extends ObsMigrator {

    @Override
    def void migrate() {

        executeMysql("Create staging table for migrating next scheduled visit date", '''
            create table hivmigration_exam_extra (                            
              source_encounter_id int,
              source_patient_id int,
              main_activity_before VARCHAR(76),
              main_activity_how_now VARCHAR(10),
              other_activities_before VARCHAR(64),
              other_activities_how_now VARCHAR(10),
              oi_now_p BOOLEAN,
              plan_extra VARCHAR(2436),
              pregnant_p BOOLEAN,
              last_period_date date,
              expected_delivery_date date,
              mothers_first_name VARCHAR(32),
              mothers_last_name VARCHAR(32),
              post_test_counseling_p BOOLEAN,
              partner_referred_for_tr_p CHAR(1),              
              next_exam_date date,
              who_stage CHAR(1),
              socioecon_encounter_id INT            
            );
        ''')

        // note any next_exam_dates not on intake and followup form
        loadFromOracleToMySql('''
            INSERT INTO hivmigration_data_warnings (openmrs_patient_id, openmrs_encounter_id, encounter_date, field_name, field_value, warning_type, warning_details, flag_for_review) values(?,?,?,?,?,?,?,?) 
            ''', '''
                select  e.patient_id as patient_id,
                        e.encounter_id as encounter_id,
                        e.encounter_date as encounter_date,
                        'next_exam_date' as field_name,
                        to_char(x.next_exam_date, 'yyyy-mm-dd') as field_value,
                        'next_exam_date found on encounter other than intake or followup',
                        CONCAT('Encounter type: ', e.type),
                        1
                from hiv_exam_extra x,hiv_encounters e, hiv_demographics_real d 
                where x.next_exam_date is not null and x.encounter_id = e.encounter_id and e.patient_id = d.patient_id
                    and e.type not in ('intake','followup')
            ''')


        // migrate all HIV_EXAM_EXTRA data from the intake and followup encounters
        loadFromOracleToMySql('''
            insert into hivmigration_exam_extra (
              source_encounter_id,
              source_patient_id,
              main_activity_before,
              main_activity_how_now,
              other_activities_before,
              other_activities_how_now,
              oi_now_p,
              plan_extra,
              pregnant_p,
              last_period_date,
              expected_delivery_date,
              mothers_first_name,
              mothers_last_name,
              post_test_counseling_p,
              partner_referred_for_tr_p,
              next_exam_date,
              who_stage 
            )
            values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) 
            ''', '''
                select 
                        x.encounter_id as source_encounter_id,
                        e.patient_id as source_patient_id,
                        x.main_activity_before,
                        x.main_activity_how_now,
                        x.other_activities_before,
                        x.other_activities_how_now,                      
                        case 
                            when (x.oi_now_p = 't') then 1 
                            when (x.oi_now_p = 'f') then 0 
                            else null 
                        end as oi_now_p,
                        x.plan_extra,                      
                        case 
                            when (x.pregnant_p = 't') then 1 
                            when (x.pregnant_p = 'f') then 0 
                            else null 
                        end as pregnant_p,
                        to_char(x.last_period_date, 'yyyy-mm-dd') as last_period_date,
                        to_char(x.expected_delivery_date, 'yyyy-mm-dd') as expected_delivery_date,
                        x.mothers_first_name,
                        x.mothers_last_name,                      
                        case 
                            when (x.post_test_counseling_p = 't') then 1 
                            when (x.post_test_counseling_p = 'f') then 0 
                            else null 
                        end as post_test_counseling_p,    
                        x.partner_referred_for_tr_p,                                                                                       
                        to_char(x.next_exam_date, 'yyyy-mm-dd') as next_exam_date,
                        x.who_stage
                from hiv_exam_extra x,hiv_encounters e, hiv_demographics_real d 
                where x.encounter_id = e.encounter_id and e.patient_id = d.patient_id
                    and e.type in ('intake','followup');
            ''')

        create_tmp_obs_table()

        executeMysql("Load HIV_EXAM_EXTRA observations", ''' 
                      
            SET @next_visit_date_concept_uuid = '3ce94df0-26fe-102b-80cb-0017a47871b2';            
                        
            INSERT INTO tmp_obs (value_datetime, source_patient_id, source_encounter_id, concept_uuid)
            SELECT next_exam_date, source_patient_id, source_encounter_id, @next_visit_date_concept_uuid
            FROM hivmigration_exam_extra
            WHERE next_exam_date is not null;      


            -- Plan Extra (Action plan) obs     
            INSERT INTO tmp_obs(
                source_encounter_id,
                concept_uuid,
                value_text)
            SELECT 
                source_encounter_id,
                concept_uuid_from_mapping('PIH', '1620') as concept_uuid,                
                plan_extra  
            FROM hivmigration_exam_extra      
            WHERE plan_extra is not null; 

            -- WHO_STAGE obs     
            INSERT INTO tmp_obs(
                source_encounter_id,
                concept_uuid,
                value_coded_uuid)
            SELECT 
                source_encounter_id,
                concept_uuid_from_mapping('PIH', 'CURRENT WHO HIV STAGE') as concept_uuid,                
                case 
                    when (who_stage= '1') then concept_uuid_from_mapping('PIH', 'WHO STAGE 1 ADULT')
                    when (who_stage= '2') then concept_uuid_from_mapping('PIH', 'WHO STAGE 2 ADULT')
                    when (who_stage= '3') then concept_uuid_from_mapping('PIH', 'WHO STAGE 3 ADULT')
                    when (who_stage= '4') then concept_uuid_from_mapping('PIH', '1207')
                    else null 
                end as value_coded_uuid  
            FROM hivmigration_exam_extra      
            WHERE who_stage in ('1', '2', '3', '4');  

            -- Pregnant
            INSERT INTO tmp_obs(
                source_encounter_id,
                concept_uuid,
                value_coded_uuid)
            SELECT 
                source_encounter_id,
                concept_uuid_from_mapping('PIH', 'PREGNANCY STATUS') as concept_uuid,                
                concept_uuid_from_mapping('PIH', 'YES') as value_coded_uuid                
            FROM hivmigration_exam_extra      
            WHERE pregnant_p = true; 

            -- Last period date
            INSERT INTO tmp_obs(
                source_encounter_id,
                concept_uuid,
                value_datetime)
            SELECT 
                source_encounter_id,
                concept_uuid_from_mapping('PIH', 'DATE OF LAST MENSTRUAL PERIOD') as concept_uuid,                
                last_period_date               
            FROM hivmigration_exam_extra      
            WHERE last_period_date is not null; 

            -- Due date
            INSERT INTO tmp_obs(
                source_encounter_id,
                concept_uuid,
                value_datetime)
            SELECT 
                source_encounter_id,
                concept_uuid_from_mapping('CIEL', '5596') as concept_uuid,                
                expected_delivery_date               
            FROM hivmigration_exam_extra      
            WHERE expected_delivery_date is not null; 


            -- Partner referred for HIV test
            INSERT INTO tmp_obs(
                source_encounter_id,
                concept_uuid,
                value_coded_uuid)
            SELECT 
                source_encounter_id,
                concept_uuid_from_mapping('PIH', 'PARTNER REFERRED FOR HIV TEST') as concept_uuid,                                                
                case 
                    when (partner_referred_for_tr_p='t') then concept_uuid_from_mapping('PIH', 'YES')
                    when (partner_referred_for_tr_p='f') then concept_uuid_from_mapping('PIH', 'NO')
                    when (partner_referred_for_tr_p='9') then concept_uuid_from_mapping('PIH', 'NO PARTNER')                    
                    else null 
                end as value_coded_uuid                
            FROM hivmigration_exam_extra      
            WHERE partner_referred_for_tr_p is not null;

            -- Post test counseling
            INSERT INTO tmp_obs(
                source_encounter_id,
                concept_uuid,
                value_coded_uuid)
            SELECT 
                source_encounter_id,
                concept_uuid_from_mapping('CIEL', '159382') as concept_uuid,                                                
                case 
                    when (post_test_counseling_p=true) then concept_uuid_from_mapping('PIH', 'YES')
                    when (post_test_counseling_p=false) then concept_uuid_from_mapping('PIH','NO')                    
                    else null 
                end as value_coded_uuid                
            FROM hivmigration_exam_extra      
            WHERE post_test_counseling_p is not null; 

            -- create new SocioEconomics encounters if an encounter was not already created during the SocioEconomicsMigrator
             set @form_id = (select form_id from form where name = 'Socioeconomics Note');
             insert into encounter(
                encounter_datetime, 
                date_created, 
                encounter_type, 
                form_id, 
                patient_id, 
                creator, 
                location_id, 
                uuid)
            select 
                h.encounter_date,
                h.date_created,
                encounter_type('Socio-economics') as encounter_type,
                @form_id as form_id,
                p.person_id,
                1,
                ifnull(h.location_id, 1) as location_id,
                uuid() as uuid
             from  hivmigration_exam_extra x   
            join hivmigration_encounters h on x.source_encounter_id = h.source_encounter_id 
            join hivmigration_patients p on p.source_patient_id = x.source_patient_id  
            where x.socioecon_encounter_id is null and (x.main_activity_before is not null or main_activity_how_now is not null or x.other_activities_before is not null or other_activities_how_now is not null) 
                and x.source_encounter_id not in (
                select x.source_encounter_id from hivmigration_exam_extra x  
                        join hivmigration_encounters h on x.source_encounter_id = h.source_encounter_id 
                        join hivmigration_patients p on p.source_patient_id = x.source_patient_id  
                        join encounter e on e.encounter_type = encounter_type('Socio-economics') and e.patient_id = p.person_id             
                        where (x.main_activity_before is not null or main_activity_how_now is not null or x.other_activities_before is not null or other_activities_how_now is not null) and date(e.encounter_datetime) = date(h.encounter_date)); 

            -- Add socioeconomics encounter_id                 
            update hivmigration_exam_extra x  
            join hivmigration_encounters h on x.source_encounter_id = h.source_encounter_id 
            join hivmigration_patients p on p.source_patient_id = x.source_patient_id  
            join encounter e on e.encounter_type = encounter_type('Socio-economics') and e.patient_id = p.person_id 
            SET x.socioecon_encounter_id = e.encounter_id 
            where (x.main_activity_before is not null or main_activity_how_now is not null or x.other_activities_before is not null or other_activities_how_now is not null) and date(e.encounter_datetime) = date(h.encounter_date);
                
            -- Main activity before illness 
            INSERT INTO tmp_obs(
                encounter_id,
                concept_uuid,
                value_text)
            SELECT 
                socioecon_encounter_id,
                concept_uuid_from_mapping('PIH', '1402') as concept_uuid,                
                main_activity_before  
            FROM hivmigration_exam_extra      
            WHERE main_activity_before is not null and socioecon_encounter_id is not null;

            -- Ability to perform main activity now 
            INSERT INTO tmp_obs(
                encounter_id,
                concept_uuid,
                value_text)
            SELECT 
                socioecon_encounter_id,
                concept_uuid_from_mapping('PIH', '11543') as concept_uuid,                
                main_activity_how_now  
            FROM hivmigration_exam_extra      
            WHERE main_activity_how_now is not null and socioecon_encounter_id is not null; 

            -- Other activities before illness 
            INSERT INTO tmp_obs(
                encounter_id,
                concept_uuid,
                value_text)
            SELECT 
                socioecon_encounter_id,
                concept_uuid_from_mapping('PIH', 'OTHER ACTIVITIES BEFORE ILLNESS') as concept_uuid,                
                other_activities_before  
            FROM hivmigration_exam_extra      
            WHERE other_activities_before is not null and socioecon_encounter_id is not null;

            -- Ability to perform other activities now 
            INSERT INTO tmp_obs(
                encounter_id,
                concept_uuid,
                value_text)
            SELECT 
                socioecon_encounter_id,
                concept_uuid_from_mapping('PIH', 'CURRENT ABILITY TO PERFORM OTHER ACTIVITIES BEFORE ILLNESS') as concept_uuid,                
                other_activities_how_now  
            FROM hivmigration_exam_extra      
            WHERE other_activities_how_now is not null and socioecon_encounter_id is not null;
        ''')

        migrate_tmp_obs()

        executeMysql("Create staging table for migrating transfer_out_to obs", '''
            create table hivmigration_transfer_out_to (   
              obs_id int PRIMARY KEY AUTO_INCREMENT,                   
              source_encounter_id int,            
              hiv_institution_id int,                         
              transfer_out_to VARCHAR(48) ,               
              obs_date date             
            );
        ''')

        setAutoIncrement("hivmigration_transfer_out_to", "(select max(obs_id)+1 from obs)")

        loadFromOracleToMySql('''
            insert into hivmigration_transfer_out_to(
                 source_encounter_id,
                 hiv_institution_id,
                 transfer_out_to,
                 obs_date)
            values (?, ?, ?, ?)
            ''', '''
            select o.encounter_id as source_encounter_id,                    
                    i.INSTITUTION_ID as hiv_institution_id,
                    i.name as transfer_out_to,
                    to_char(o.entry_date, 'yyyy-mm-dd') as obs_date 
            from hiv_observations o, hiv_encounters e, hiv_demographics_real d, hiv_institutions i  
            where o.ENCOUNTER_ID = e.ENCOUNTER_ID and e.patient_id = d.patient_id and o.VALUE = i.INSTITUTION_ID 
                    and observation='transfer_out_to' and o.value is not null 
            order by o.ENCOUNTER_ID; 
        ''')

        create_tmp_obs_table()
        setAutoIncrement("tmp_obs", "(select max(obs_id)+1 from hivmigration_transfer_out_to)")

        executeMysql("Load transferred_out observations", ''' 
                                                       
            -- Create Transfer out to another location obs_group
            INSERT INTO tmp_obs (
                obs_id,
                source_encounter_id, 
                concept_uuid)
            SELECT 
                obs_id,
                source_encounter_id,
                concept_uuid_from_mapping('PIH', '13170') as concept_uuid
            FROM hivmigration_transfer_out_to
            WHERE hiv_institution_id is not null;    

            -- Create HUM Disposition categories obs
            INSERT INTO tmp_obs (
                obs_group_id,
                source_encounter_id,
                concept_uuid,
                value_coded_uuid)
            SELECT 
                obs_id,
                source_encounter_id,
                concept_uuid_from_mapping('PIH', 'HUM Disposition categories') as concept_uuid,
                concept_uuid_from_mapping('PIH', 'PATIENT TRANSFERRED OUT') as value_coded_uuid
            FROM hivmigration_transfer_out_to
            WHERE hiv_institution_id is not null;  
            
            -- External transfers                        
            INSERT INTO tmp_obs (
                obs_group_id,
                source_encounter_id,
                concept_uuid,
                value_coded_uuid)
            SELECT 
                obs_id,
                source_encounter_id,
                concept_uuid_from_mapping('PIH', 'Transfer out location') as concept_uuid, 
                concept_uuid_from_mapping('PIH', 'Non-ZL supported site') as value_coded_uuid
            FROM hivmigration_transfer_out_to
            WHERE transfer_out_to = 'Autre (non-ZL)';
            
            -- ZL-supported site transfers                    
            INSERT INTO tmp_obs (
                obs_group_id,
                source_encounter_id,
                concept_uuid,
                value_coded_uuid)
            SELECT 
                obs_id,
                source_encounter_id,
                concept_uuid_from_mapping('PIH', 'Transfer out location') as concept_uuid,
                concept_uuid_from_mapping('PIH', 'ZL-supported site') as value_coded_uuid
            FROM hivmigration_transfer_out_to
            WHERE transfer_out_to != 'Autre (non-ZL)' or transfer_out_to is null;
            
            -- ZL-supported site name            
            INSERT INTO tmp_obs (
                obs_group_id,
                source_encounter_id,
                concept_uuid,
                value_text,
                comments)
            SELECT 
                t.obs_id,
                t.source_encounter_id,
                concept_uuid_from_mapping('PIH', '8621') as concept_uuid,
                c.openmrs_id as value_text,
                'org.openmrs.Location'
            FROM hivmigration_transfer_out_to t, hivmigration_health_center c 
            WHERE (t.transfer_out_to != 'Autre (non-ZL)' or transfer_out_to is null) and t.hiv_institution_id = c.hiv_emr_id;
                   
        ''')

        migrate_tmp_obs()

        executeMysql("Create staging table for migrating transfer_in_from obs", '''
            create table hivmigration_transfer_in_from (   
              obs_id int PRIMARY KEY AUTO_INCREMENT,                         
              source_encounter_id int,
              source_patient_id int,           
              transfer_p VARCHAR(1),                 
              transfer_in_from VARCHAR(48),               
              obs_date date             
            );
        ''')

        setAutoIncrement("hivmigration_transfer_in_from", "(select max(obs_id)+1 from obs)")

        loadFromOracleToMySql('''
            insert into hivmigration_transfer_in_from
                (source_encounter_id,
                 source_patient_id,
                 transfer_p,
                 transfer_in_from,
                 obs_date)
            values (?, ?, ?, ?, ?)
            ''', '''
            select o.encounter_id as source_encounter_id,
                    e.patient_id as source_patient_id,                         
                    o.value as transfer_p,
                    case when (o.value='t') then 
                    ( select obs.value from hiv_observations obs
                      where  obs.observation='transfer_in_from' and obs.encounter_id= o.encounter_id)                          
                    end as transfer_in_from,
                    to_char(o.entry_date, 'yyyy-mm-dd') as obs_date 
            from hiv_observations o, hiv_encounters e, hiv_demographics_real d  
            where o.ENCOUNTER_ID = e.ENCOUNTER_ID and e.patient_id = d.patient_id  
                    and observation='transfer_p' and o.value = 't' 
            order by o.ENCOUNTER_ID
        ''')

        create_tmp_obs_table()
        setAutoIncrement("tmp_obs", "(select max(obs_id)+1 from hivmigration_transfer_in_from)")

        executeMysql("Load transfer_in_from observations", ''' 
                      
            -- Create Referral from location construct
            INSERT INTO tmp_obs(
                obs_id,                 
                source_encounter_id, 
                concept_uuid) 
            SELECT 
                obs_id,                 
                source_encounter_id, 
                concept_uuid_from_mapping('PIH', '13169') as concept_uuid
            FROM hivmigration_transfer_in_from 
            WHERE transfer_p = 't';
            
            --  Add Transfer in to the construct
            INSERT INTO tmp_obs(
                obs_group_id,                 
                source_encounter_id, 
                concept_uuid,
                value_coded_uuid) 
            SELECT 
                obs_id,                 
                source_encounter_id, 
                concept_uuid_from_mapping('PIH', '13712') as concept_uuid,
                concept_uuid_from_mapping('PIH', '6965') as value_coded_uuid
            FROM hivmigration_transfer_in_from 
            WHERE transfer_p = 't';
            
            --  Add Referred from another site to the construct
            INSERT INTO tmp_obs(
                obs_group_id,                 
                source_encounter_id, 
                concept_uuid,
                value_coded_uuid) 
            SELECT 
                obs_id,                 
                source_encounter_id, 
                concept_uuid_from_mapping('PIH', '6401') as concept_uuid,
                concept_uuid_from_mapping('PIH', '8856') as value_coded_uuid
            FROM hivmigration_transfer_in_from 
            WHERE transfer_in_from is not null;
            
            --  Add the Name of external location to the construct
            INSERT INTO tmp_obs(
                obs_group_id,                 
                source_encounter_id, 
                concept_uuid,
                value_text) 
            SELECT 
                obs_id,                 
                source_encounter_id, 
                concept_uuid_from_mapping('PIH', '11483') as concept_uuid,
                transfer_in_from 
            FROM hivmigration_transfer_in_from 
            WHERE transfer_in_from is not null;
            
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
        executeMysql("drop table if exists hivmigration_transfer_in_from")
        executeMysql("drop table if exists hivmigration_tb_screening")
    }
}
