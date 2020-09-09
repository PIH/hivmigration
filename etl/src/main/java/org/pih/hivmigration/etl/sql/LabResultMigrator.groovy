package org.pih.hivmigration.etl.sql

class LabResultMigrator extends SqlMigrator {

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

        executeMysql("Create stored procedures to add obs", '''
            DROP PROCEDURE IF EXISTS create_tmp_obs_table;
            DROP PROCEDURE IF EXISTS migrate_tmp_obs;
            DELIMITER $$ ;
            CREATE PROCEDURE create_tmp_obs_table()
            BEGIN
                DROP TABLE IF EXISTS tmp_obs;
                CREATE TABLE tmp_obs (
                    obs_id INT PRIMARY KEY AUTO_INCREMENT,
                    obs_group_id INT,
                    source_patient_id INT,
                    source_encounter_id INT,
                    concept_uuid CHAR(38),
                    value_coded_uuid CHAR(38),
                    value_datetime DATETIME,
                    value_numeric DOUBLE,
                    value_text TEXT
                );
            END $$
            CREATE PROCEDURE migrate_tmp_obs()
            BEGIN
                INSERT INTO obs (
                    obs_id, person_id, encounter_id, obs_group_id, obs_datetime, location_id, concept_id,
                    value_coded, value_numeric, value_datetime, value_text, creator, date_created, voided, uuid
                )
                SELECT
                    o.obs_id, p.person_id, e.encounter_id, o.obs_group_id, e.encounter_date, e.location_id, q.concept_id,
                    a.concept_id, o.value_numeric, o.value_datetime, o.value_text, 1, e.date_created, 0, uuid()
                FROM tmp_obs o
                    JOIN       hivmigration_patients p ON o.source_patient_id = p.source_patient_id
                    JOIN       hivmigration_encounters e ON o.source_encounter_id = e.source_encounter_id
                    LEFT JOIN  concept q ON q.uuid = o.concept_uuid
                    LEFT JOIN  concept a ON a.uuid = o.value_coded_uuid;
            END $$
            DELIMITER ;
        ''')

        executeMysql("Load lab results as observations",
        '''
            CALL create_tmp_obs_table();

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
                (obs_group_id, value_text, source_patient_id, source_encounter_id, concept_uuid)
            SELECT obs_id, value_numeric, source_patient_id, source_encounter_id, '3cd4a882-26fe-102b-80cb-0017a47871b2'
            FROM hivmigration_lab_results
            WHERE test_type = 'viral_load' AND value_numeric IS NOT NULL;
            
            -- Detectable
            INSERT INTO tmp_obs
                (obs_group_id, value_coded_uuid, source_patient_id, source_encounter_id, concept_uuid)
            SELECT obs_id, '1302AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', source_patient_id, source_encounter_id, '1305AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'
            FROM hivmigration_lab_results
            WHERE test_type = 'viral_load' AND vl_beyond_detectable_limit = 1; 
            
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
            INSERT INTO tmp_obs (value_coded_uuid, source_patient_id, source_encounter_id, concept_uuid)
            SELECT
                IF(value_boolean=1, '3cd3a7a2-26fe-102b-80cb-0017a47871b2', '3cd28732-26fe-102b-80cb-0017a47871b2'),
                source_patient_id,
                source_encounter_id,
                '3cd6c946-26fe-102b-80cb-0017a47871b2'
            FROM hivmigration_lab_results
            WHERE test_type = 'tr';
            
            CALL migrate_tmp_obs();
        ''')
    }

    @Override
    def void revert() {
        executeMysql("drop table hivmigration_lab_results")
        clearTable("obs")
    }
}
