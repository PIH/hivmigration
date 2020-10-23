package org.pih.hivmigration.etl.sql

class LabResultMigrator extends ObsMigrator {

    @Override
    def void migrate() {
        executeMysql("Create staging table", '''
            create table hivmigration_lab_results (
              obs_id int PRIMARY KEY AUTO_INCREMENT,
              source_patient_id int,
              source_encounter_id int,
              source_result_id int,
              sample_id VARCHAR(20),
              test_type VARCHAR(16), -- viral_load, CD4, tr, ppd, hematocrit
              obs_datetime date,
              value_numeric DOUBLE,
              value_text VARCHAR(100),
              value_boolean BOOLEAN,
              vl_beyond_detectable_limit BOOLEAN,
              vl_detectable_lower_limit DOUBLE,
              KEY `source_encounter_id_idx` (`source_encounter_id`)
            );
        ''')

        loadFromOracleToMySql('''
            insert into hivmigration_lab_results
               (source_patient_id,
                source_encounter_id,
                source_result_id,
                sample_id,
                test_type,
                obs_datetime,
                value_numeric,
                value_text,
                value_boolean,
                vl_beyond_detectable_limit,
                vl_detectable_lower_limit)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', '''
            select   e.patient_id as source_patient_id,
                     e.ENCOUNTER_ID as source_encounter_id, 
                     r.RESULT_ID as source_result_id,
                     r.sample_id, 
                     lower(t.name) as test_type,
                     to_char(e.encounter_date, 'yyyy-mm-dd') as obs_datetime,
                     value as value_numeric, 
                     value_string as value_text,   
                     case when (value_p='t') then ( 1)     
                        else 0
                        end as value_boolean,                
                     case when (r.RESULT_UNDETECTABLE='t') then ( 1)     
                        else 0
                        end as vl_beyond_detectable_limit,                  
                     r.RESULT_UNDETECTABLE_RANGE as vl_detectable_lower_limit
            from     hiv_demographics_real d, hiv_encounters e, hiv_lab_results r, hiv_lab_tests t
            where    d.patient_id = e.patient_id
            and      e.encounter_id = r.encounter_id
            and      r.lab_test_id = t.lab_test_id  
            order by d.patient_id desc, e.encounter_id desc
        ''')

        loadFromOracleToMySql('''
            insert into hivmigration_lab_results
               (source_patient_id,
                source_encounter_id,                                
                test_type,
                value_boolean,
                obs_datetime)
            values (?, ?, ?, ?, ?)
        ''', '''
            select e.PATIENT_ID as source_patient_id,  
                    e.ENCOUNTER_ID as source_encounter_id, 
                    r.LAB_TEST,
                    case when ( r.RESULT='t' ) then 1 else 0 end as value_boolean,
                    case when (r.test_date is null) then to_char(e.encounter_date, 'yyyy-mm-dd') 
                    else to_char(r.test_date, 'yyyy-mm-dd') 
                    end as obs_datetime
            from  HIV_EXAM_LAB_RESULTS r, hiv_encounters e 
            where r.encounter_id = e.encounter_id and LAB_TEST='tr' and r.RESULT is not null
        ''')

        create_tmp_obs_table()

        executeMysql("Load lab results as observations",
        '''
            -- Viral load
            --
            
            -- Create construct
            INSERT INTO tmp_obs (obs_id, source_patient_id, source_encounter_id, concept_uuid)
            SELECT obs_id, source_patient_id, source_encounter_id, '11765b8c-a338-48a4-9480-df898c903723'
            FROM hivmigration_lab_results
            WHERE test_type = 'viral_load';
            
            -- Specimen number
            INSERT INTO tmp_obs
                (obs_group_id, value_text, source_patient_id, source_encounter_id, concept_uuid)
            SELECT obs_id, sample_id, source_patient_id, source_encounter_id, '162086AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'
            FROM hivmigration_lab_results
            WHERE test_type = 'viral_load' AND sample_id IS NOT NULL;

            -- HVL Value
            INSERT INTO tmp_obs
                (obs_group_id, value_numeric, source_patient_id, source_encounter_id, concept_uuid)
            SELECT obs_id, value_numeric, source_patient_id, source_encounter_id, '3cd4a882-26fe-102b-80cb-0017a47871b2'
            FROM hivmigration_lab_results
            WHERE test_type = 'viral_load' AND value_numeric IS NOT NULL;
            
            -- Below detectable limit
            INSERT INTO tmp_obs
                (obs_group_id, value_coded_uuid, source_patient_id, source_encounter_id, concept_uuid)
            SELECT obs_id, '1306AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', source_patient_id, source_encounter_id, '1305AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'
            FROM hivmigration_lab_results
            WHERE test_type = 'viral_load' AND vl_beyond_detectable_limit = 1; 
            
            -- Detected
            INSERT INTO tmp_obs
                (obs_group_id, value_coded_uuid, source_patient_id, source_encounter_id, concept_uuid)
            SELECT obs_id, '1301AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', source_patient_id, source_encounter_id, '1305AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'
            FROM hivmigration_lab_results
            WHERE test_type = 'viral_load' AND vl_beyond_detectable_limit = 0; 
            
            -- Detectable lower limit
            INSERT INTO tmp_obs
                (obs_group_id, value_numeric, source_patient_id, source_encounter_id, concept_uuid)
            SELECT obs_id, vl_detectable_lower_limit, source_patient_id, source_encounter_id, '53cb83ed-5d55-4b63-922f-d6b8fc67a5f8'
            FROM hivmigration_lab_results
            WHERE test_type = 'viral_load' AND vl_detectable_lower_limit IS NOT NULL;
            
                       
            -- CD4
            --
            INSERT INTO tmp_obs (value_numeric, source_patient_id, source_encounter_id, concept_uuid)
            SELECT value_numeric, source_patient_id, source_encounter_id, '3ceda710-26fe-102b-80cb-0017a47871b2'
            FROM hivmigration_lab_results
            WHERE test_type = 'cd4';
            
            -- Hematocrit
            --
            INSERT INTO tmp_obs (value_numeric, source_patient_id, source_encounter_id, concept_uuid)
            SELECT value_numeric, source_patient_id, source_encounter_id, '3cd69a98-26fe-102b-80cb-0017a47871b2'
            FROM hivmigration_lab_results
            WHERE test_type = 'hematocrit';
                        
            -- PPD
            --
            INSERT INTO tmp_obs (value_numeric, source_patient_id, source_encounter_id, concept_uuid)
            SELECT value_numeric, source_patient_id, source_encounter_id, '3cecf388-26fe-102b-80cb-0017a47871b2'
            FROM hivmigration_lab_results
            WHERE test_type = 'ppd';

            -- Rapid Test
            -- 
            -- HIV rapid test set construct
            INSERT INTO tmp_obs (obs_id, source_patient_id, source_encounter_id, concept_uuid)
            SELECT
                obs_id,                     
                source_patient_id,
                source_encounter_id,
                concept_uuid_from_mapping('PIH', '13135')
            FROM hivmigration_lab_results
            WHERE test_type = 'tr'; 
            
            INSERT INTO tmp_obs (value_coded_uuid, source_patient_id, source_encounter_id, concept_uuid)
            SELECT  
                IF(value_boolean=1, concept_uuid_from_mapping('CIEL', '703'), concept_uuid_from_mapping('CIEL', '664')),
                source_patient_id,
                source_encounter_id,
                concept_uuid_from_mapping('CIEL', '163722')
            FROM hivmigration_lab_results
            WHERE test_type = 'tr';            
        ''')

        migrate_tmp_obs()
    }

    @Override
    def void revert() {
        executeMysql("drop table if exists hivmigration_lab_results")
        clearTable("obs")
    }
}
