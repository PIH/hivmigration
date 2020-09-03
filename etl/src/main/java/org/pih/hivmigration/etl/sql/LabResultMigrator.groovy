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

        executeMysql("Load viral load results as observations", '''
            SET @specimen_number = (SELECT concept_id FROM concept WHERE uuid='162086AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA');
            
            --
            -- Viral Load
            --
            SET @hiv_vl_construct = (SELECT concept_id FROM concept WHERE uuid='11765b8c-a338-48a4-9480-df898c903723');
            
            -- Create construct
            INSERT INTO obs(obs_id, person_id, concept_id, encounter_id, obs_datetime, location_id, creator, date_created, voided, uuid)
            SELECT r.obs_id, p.person_id, @hiv_vl_construct, e.encounter_id, r.obs_datetime, e.location_id, 1, now(), 0, r.uuid
            FROM hivmigration_lab_results r
                JOIN hivmigration_patients p ON r.source_patient_id = p.source_patient_id
                JOIN hivmigration_encounters e ON r.source_encounter_id = e.source_encounter_id
                WHERE r.test_type ='viral_load';
            
            -- Add specimen number
            INSERT INTO obs(person_id, concept_id, encounter_id, obs_datetime, location_id, obs_group_id, value_text, creator, date_created, voided, uuid)
            SELECT p.person_id, @specimen_number, e.encounter_id, r.obs_datetime, e.location_id, r.obs_id, r.sample_id,1, now(), 0, uuid()
            FROM hivmigration_lab_results r
                     JOIN hivmigration_patients p ON r.source_patient_id = p.source_patient_id
                     JOIN hivmigration_encounters e ON r.source_encounter_id = e.source_encounter_id
            WHERE r.test_type ='viral_load';
            
            -- Add numeric VL values
            SET @hvl_value = (SELECT concept_id FROM concept WHERE uuid='3cd4a882-26fe-102b-80cb-0017a47871b2');
            INSERT INTO obs(person_id, concept_id, encounter_id, obs_datetime, location_id, obs_group_id, value_numeric, creator, date_created, voided, uuid)
            SELECT p.person_id, @hvl_value, e.encounter_id, r.obs_datetime, e.location_id, r.obs_id, r.value_numeric,1, now(), 0, uuid()
            FROM hivmigration_lab_results r
                     JOIN hivmigration_patients p ON r.source_patient_id = p.source_patient_id
                     JOIN hivmigration_encounters e ON r.source_encounter_id = e.source_encounter_id
            WHERE r.value_numeric IS NOT NULL AND r.test_type ='viral_load';
            
            -- Add undetectable VL values
            SET @hvl_qualitative = (SELECT concept_id FROM concept WHERE uuid='1305AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA');
            SET @not_detected = (SELECT concept_id FROM concept WHERE uuid='1302AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA');
            INSERT INTO obs(person_id, concept_id, encounter_id, obs_datetime, location_id, obs_group_id, value_coded, creator, date_created, voided, uuid)
            SELECT p.person_id, @hvl_qualitative, e.encounter_id, r.obs_datetime, e.location_id, r.obs_id, @not_detected,1, now(), 0, uuid()
            FROM hivmigration_lab_results r
                     JOIN hivmigration_patients p ON r.source_patient_id = p.source_patient_id
                     JOIN hivmigration_encounters e ON r.source_encounter_id = e.source_encounter_id
            WHERE r.vl_beyond_detectable_limit = 1 AND r.test_type ='viral_load';
            
            -- Add Detectable Lower Limit values
            SET @detectable_lower_limit = (SELECT concept_id FROM concept WHERE uuid='53cb83ed-5d55-4b63-922f-d6b8fc67a5f8');
            INSERT INTO obs(person_id, concept_id, encounter_id, obs_datetime, location_id, obs_group_id, value_numeric, creator, date_created, voided, uuid)
            SELECT p.person_id, @detectable_lower_limit, e.encounter_id, r.obs_datetime, e.location_id, r.obs_id, r.vl_detectable_lower_limit,1, now(), 0, uuid()
            FROM hivmigration_lab_results r
                     JOIN hivmigration_patients p ON r.source_patient_id = p.source_patient_id
                     JOIN hivmigration_encounters e ON r.source_encounter_id = e.source_encounter_id
            WHERE r.vl_detectable_lower_limit IS NOT NULL AND r.test_type ='viral_load';
            
            
            --
            -- CD4 Count
            --
            SET @cd4_construct = (SELECT concept_id FROM concept WHERE uuid='37769FDB-5FC1-4D47-82C2-DB88960BB224');
            INSERT INTO obs(obs_id, person_id, concept_id, encounter_id, obs_datetime, location_id, creator, date_created, voided, uuid)
            SELECT r.obs_id, p.person_id, @cd4_construct, e.encounter_id, r.obs_datetime, e.location_id, 1, now(), 0, r.uuid
            FROM hivmigration_lab_results r
                JOIN hivmigration_patients p ON r.source_patient_id = p.source_patient_id
                JOIN hivmigration_encounters e ON r.source_encounter_id = e.source_encounter_id
                WHERE r.test_type ='cd4';
            
            -- Add specimen number
            INSERT INTO obs(person_id, concept_id, encounter_id, obs_datetime, location_id, obs_group_id, value_text, creator, date_created, voided, uuid)
            SELECT p.person_id, @specimen_number, e.encounter_id, r.obs_datetime, e.location_id, r.obs_id, r.sample_id,1, now(), 0,  uuid()
            FROM hivmigration_lab_results r
                     JOIN hivmigration_patients p ON r.source_patient_id = p.source_patient_id
                     JOIN hivmigration_encounters e ON r.source_encounter_id = e.source_encounter_id
            WHERE r.test_type ='cd4';
            
            -- Add numeric value
            SET @cd4_value = (SELECT concept_id FROM concept WHERE uuid='3ceda710-26fe-102b-80cb-0017a47871b2');
            INSERT INTO obs(person_id, concept_id, encounter_id, obs_datetime, location_id, obs_group_id, value_numeric, creator, date_created, voided, uuid)
            SELECT p.person_id, @cd4_value, e.encounter_id, r.obs_datetime, e.location_id, r.obs_id, r.value_numeric,1, now(), 0, uuid()
            FROM hivmigration_lab_results r
                     JOIN hivmigration_patients p ON r.source_patient_id = p.source_patient_id
                     JOIN hivmigration_encounters e ON r.source_encounter_id = e.source_encounter_id
            WHERE r.value_numeric IS NOT NULL AND r.test_type ='cd4';
            
            
            --
            -- Hematocrit
            --
            SET @hematocrit_construct = (SELECT concept_id FROM concept WHERE uuid='267C165C-1B8F-48FE-91AC-C1AE8C7412A0');
            INSERT INTO obs(obs_id, person_id, concept_id, encounter_id, obs_datetime, location_id, creator, date_created, voided, uuid)
            SELECT r.obs_id, p.person_id, @hematocrit_construct, e.encounter_id, r.obs_datetime, e.location_id, 1, now(), 0, r.uuid
            FROM hivmigration_lab_results r
                JOIN hivmigration_patients p ON r.source_patient_id = p.source_patient_id
                JOIN hivmigration_encounters e ON r.source_encounter_id = e.source_encounter_id
                WHERE r.test_type ='hematocrit';
            
            -- Specimen Number
            INSERT INTO obs(person_id, concept_id, encounter_id, obs_datetime, location_id, obs_group_id, value_text, creator, date_created, voided, uuid)
            SELECT p.person_id, @specimen_number, e.encounter_id, r.obs_datetime, e.location_id, r.obs_id, r.sample_id,1, now(), 0,  uuid()
            FROM hivmigration_lab_results r
                     JOIN hivmigration_patients p ON r.source_patient_id = p.source_patient_id
                     JOIN hivmigration_encounters e ON r.source_encounter_id = e.source_encounter_id
            WHERE r.test_type ='hematocrit';
            
            -- Numeric Value
            SET @hematocrit_value = (SELECT concept_id FROM concept WHERE uuid='3cd69a98-26fe-102b-80cb-0017a47871b2');
            INSERT INTO obs(person_id, concept_id, encounter_id, obs_datetime, location_id, obs_group_id, value_numeric, creator, date_created, voided, uuid)
            SELECT p.person_id, @hematocrit_value, e.encounter_id, r.obs_datetime, e.location_id, r.obs_id, r.value_numeric,1, now(), 0, uuid()
            FROM hivmigration_lab_results r
                     JOIN hivmigration_patients p ON r.source_patient_id = p.source_patient_id
                     JOIN hivmigration_encounters e ON r.source_encounter_id = e.source_encounter_id
            WHERE r.value_numeric IS NOT NULL AND r.test_type ='hematocrit';
            
            
            --
            -- PPD
            --
            SET @ppd_construct = (SELECT concept_id FROM concept WHERE uuid='ACF90DED-B595-4356-9840-788094C60AFB');
            INSERT INTO obs(obs_id, person_id, concept_id, encounter_id, obs_datetime, location_id, creator, date_created, voided, uuid)
            SELECT r.obs_id, p.person_id, @ppd_construct, e.encounter_id, r.obs_datetime, e.location_id, 1, now(), 0, r.uuid
            FROM hivmigration_lab_results r
                JOIN hivmigration_patients p ON r.source_patient_id = p.source_patient_id
                JOIN hivmigration_encounters e ON r.source_encounter_id = e.source_encounter_id
                WHERE r.test_type ='ppd';
            
            -- Specimen number
            INSERT INTO obs(person_id, concept_id, encounter_id, obs_datetime, location_id, obs_group_id, value_text, creator, date_created, voided, uuid)
            SELECT p.person_id, @specimen_number, e.encounter_id, r.obs_datetime, e.location_id, r.obs_id, r.sample_id,1, now(), 0,  uuid()
            FROM hivmigration_lab_results r
                     JOIN hivmigration_patients p ON r.source_patient_id = p.source_patient_id
                     JOIN hivmigration_encounters e ON r.source_encounter_id = e.source_encounter_id
            WHERE r.test_type ='ppd';
            
            -- Numeric Value
            SET @ppd_value = (SELECT concept_id FROM concept WHERE uuid='3cecf388-26fe-102b-80cb-0017a47871b2');
            
            INSERT INTO obs(person_id, concept_id, encounter_id, obs_datetime, location_id, obs_group_id, value_numeric, creator, date_created, voided, uuid)
            SELECT p.person_id, @ppd_value, e.encounter_id, r.obs_datetime, e.location_id, r.obs_id, r.value_numeric,1, now(), 0, uuid()
            FROM hivmigration_lab_results r
                     JOIN hivmigration_patients p ON r.source_patient_id = p.source_patient_id
                     JOIN hivmigration_encounters e ON r.source_encounter_id = e.source_encounter_id
            WHERE r.value_numeric IS NOT NULL AND r.test_type ='ppd';
            
            
            --
            -- Rapid Test
            --
            SET @test_rapid_construct = (SELECT concept_id FROM concept WHERE uuid='A19F5A83-E960-413D-B93B-9270C53580A2');
            INSERT INTO obs(obs_id, person_id, concept_id, encounter_id, obs_datetime, location_id, creator, date_created, voided, uuid)
            SELECT r.obs_id, p.person_id, @test_rapid_construct, e.encounter_id, r.obs_datetime, e.location_id, 1, now(), 0, r.uuid
            FROM hivmigration_lab_results r
                JOIN hivmigration_patients p ON r.source_patient_id = p.source_patient_id
                JOIN hivmigration_encounters e ON r.source_encounter_id = e.source_encounter_id
                WHERE r.test_type ='tr';
            
            -- Specimen number
            INSERT INTO obs(person_id, concept_id, encounter_id, obs_datetime, location_id, obs_group_id, value_text, creator, date_created, voided, uuid)
            SELECT p.person_id, @specimen_number, e.encounter_id, r.obs_datetime, e.location_id, r.obs_id, r.sample_id,1, now(), 0,  uuid()
            FROM hivmigration_lab_results r
                JOIN hivmigration_patients p ON r.source_patient_id = p.source_patient_id
                JOIN hivmigration_encounters e ON r.source_encounter_id = e.source_encounter_id
                WHERE r.test_type ='tr';
            
            -- Coded answer
            SET @test_rapid_value = (SELECT concept_id FROM concept WHERE uuid='3cd6c946-26fe-102b-80cb-0017a47871b2');
            SET @positive = (SELECT concept_id FROM concept WHERE uuid='3cd3a7a2-26fe-102b-80cb-0017a47871b2');
            SET @negative = (SELECT concept_id FROM concept WHERE uuid='3cd28732-26fe-102b-80cb-0017a47871b2');
            INSERT INTO obs(person_id, concept_id, encounter_id, obs_datetime, location_id, obs_group_id, value_coded, creator, date_created, voided, uuid)
            SELECT p.person_id, @test_rapid_value, e.encounter_id, r.obs_datetime, e.location_id, r.obs_id,
                   CASE WHEN ( r.value_boolean=1) THEN (@positive)
                        ELSE @negative
                       END AS value_coded
                    ,1, now(), 0, uuid()
            FROM hivmigration_lab_results r
                JOIN hivmigration_patients p ON r.source_patient_id = p.source_patient_id
                JOIN hivmigration_encounters e ON r.source_encounter_id = e.source_encounter_id
            WHERE r.test_type ='tr';
        ''')
    }

    @Override
    def void revert() {
        executeMysql("drop table hivmigration_lab_results")
        clearTable("obs")
    }
}
