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
              tmp_vl_beyond_detectable_limit_concept int,
              tmp_rapid_test_value int,
              uuid char(38),
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

        executeMysql("Add UUIDs", "UPDATE hivmigration_lab_results SET uuid = uuid();")

        executeMysql("Create stored procedure to add obs", '''
            DROP PROCEDURE IF EXISTS create_obs;
            DELIMITER $$ ;
            CREATE PROCEDURE create_obs (IN concept_uuid CHAR(38), IN test_type VARCHAR(100), IN input_column VARCHAR(100), IN output_column VARCHAR(100), IN group_column VARCHAR(100))
            BEGIN
                SET @s = CONCAT(
                    'INSERT INTO obs(`', output_column, '`, ', IF(group_column IS NULL, '', 'obs_group_id, '),
                    '   person_id, concept_id, ',
                    '   encounter_id, obs_datetime, location_id, creator, date_created, voided, uuid) ',
                    'SELECT r.`', input_column, '`, ', IF(group_column IS NULL, '', CONCAT('r.`', group_column, '`, ')),
                    '   p.person_id, (SELECT concept_id FROM concept WHERE uuid=', QUOTE(concept_uuid), '), ',
                    '   e.encounter_id, r.obs_datetime, e.location_id, 1, now(), 0, uuid() ',
                    'FROM hivmigration_lab_results r ',
                    '     JOIN hivmigration_patients p ON r.source_patient_id = p.source_patient_id ',
                    '     JOIN hivmigration_encounters e ON r.source_encounter_id = e.source_encounter_id ',
                    'WHERE r.`', input_column, '` IS NOT NULL AND r.test_type = ', QUOTE(test_type));
                PREPARE stmt FROM @s;
                EXECUTE stmt;
                DEALLOCATE PREPARE stmt;
            END $$
            DELIMITER ;
        ''')

        executeMysql("Load lab results as observations",
        '''
            -- Viral load
            --
            
            -- Create construct
            SET @hiv_vl_construct = (SELECT concept_id FROM concept WHERE uuid='11765b8c-a338-48a4-9480-df898c903723');
            INSERT INTO obs(obs_id, person_id, concept_id, encounter_id, obs_datetime, location_id, creator, date_created, voided, uuid)
            SELECT r.obs_id, p.person_id, @hiv_vl_construct, e.encounter_id, r.obs_datetime, e.location_id, 1, now(), 0, r.uuid
            FROM hivmigration_lab_results r
                     JOIN hivmigration_patients p ON r.source_patient_id = p.source_patient_id
                     JOIN hivmigration_encounters e ON r.source_encounter_id = e.source_encounter_id
            WHERE r.test_type = 'viral_load';
            
            -- Specimen number
            CALL create_obs('162086AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 'viral_load', 'sample_id', 'value_text', 'obs_id');
            
            -- HVL Value
            CALL create_obs('3cd4a882-26fe-102b-80cb-0017a47871b2', 'viral_load', 'value_numeric', 'value_numeric', 'obs_id'); 
            
            -- Detectable
            UPDATE hivmigration_lab_results
            SET tmp_vl_beyond_detectable_limit_concept = (SELECT concept_id FROM concept WHERE uuid='1302AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA')
            WHERE vl_beyond_detectable_limit = 1;
            CALL create_obs('1305AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 'viral_load', 'tmp_vl_beyond_detectable_limit_concept', 'value_coded', 'obs_id');
            
            -- Detectable lower limit
            CALL create_obs('53cb83ed-5d55-4b63-922f-d6b8fc67a5f8', 'viral_load', 'vl_detectable_lower_limit', 'value_numeric', 'obs_id');
            
            
            -- CD4 Count
            --
            CALL create_obs('3ceda710-26fe-102b-80cb-0017a47871b2', 'cd4', 'value_numeric', 'value_numeric', NULL);
            
            -- Hematocrit
            --
            CALL create_obs('3cd69a98-26fe-102b-80cb-0017a47871b2', 'hematocrit', 'value_numeric', 'value_numeric', NULL);
            
            -- PPD
            --
            CALL create_obs('3cecf388-26fe-102b-80cb-0017a47871b2', 'ppd', 'value_numeric', 'value_numeric', NULL);            
            
            -- Rapid Test
            --
            SET @positive = (SELECT concept_id FROM concept WHERE uuid='3cd3a7a2-26fe-102b-80cb-0017a47871b2');
            SET @negative = (SELECT concept_id FROM concept WHERE uuid='3cd28732-26fe-102b-80cb-0017a47871b2');
            UPDATE hivmigration_lab_results
            SET tmp_rapid_test_value = IF(value_boolean=1, @positive, @negative)
            WHERE test_type ='tr';
            CALL create_obs('3cd6c946-26fe-102b-80cb-0017a47871b2', 'tr', 'tmp_rapid_test_value', 'value_coded', NULL);
        ''')
    }

    @Override
    def void revert() {
        executeMysql("drop table hivmigration_lab_results")
        clearTable("obs")
    }
}
