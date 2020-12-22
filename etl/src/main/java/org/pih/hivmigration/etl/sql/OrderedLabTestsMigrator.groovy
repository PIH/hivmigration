package org.pih.hivmigration.etl.sql

class OrderedLabTestsMigrator extends ObsMigrator{

    @Override
    def void migrate() {
        executeMysql("Create staging table for HIV_ORDERED_LAB_TESTS", '''
            create table hivmigration_ordered_lab_tests (      
              obs_id int PRIMARY KEY AUTO_INCREMENT,                                    
              source_encounter_id int,
              source_patient_id int,
              test VARCHAR(32),
              test_other VARCHAR(96)    
            );
        ''')

        executeMysql("Create table for mapping HIV Ordered Lab Tests to OpenMRS Concepts", '''
            create table hivmigration_ordered_lab_tests_mapping (                            
              test VARCHAR(32) PRIMARY KEY,
              openmrs_concept_source VARCHAR(32),
              openmrs_concept_code VARCHAR(64)           
            );
        ''')

        executeMysql("Add HIV Ordered Lab Tests mappings", '''
            insert into hivmigration_ordered_lab_tests_mapping(
                test,
                openmrs_concept_source,
                openmrs_concept_code) 
            values
                ('abdominal_ultrasound', 'CIEL', '845'),
                ('biochemistry', 'CIEL', ''),
                ('bun', 'PIH', '12655'),
                ('cd4', 'CIEL', '5497'),
                ('chest_xray', 'CIEL', '165152'),
                ('culture', 'CIEL', '159982'),
                ('creatinine', 'CIEL', '164364'),
                ('cxr', 'CIEL', '165152'),
                ('elisa', 'CIEL', '1041'),
                ('esr', 'PIH', '1477'),
                ('glucose', 'CIEL', '887'),
                ('hematocrit', 'PIH', 'HEMATOCRIT'),
                ('hemoglobin', 'PIH', 'HEMOGLOBIN'),
                ('malaria_smear', 'CIEL', '32'),
                ('platelets', 'CIEL', '729'),
                ('ppd', 'CIEL', '5475'),
                ('pregnancy_test', 'PIH', 'B-HCG'),
                ('rpr', 'CIEL', '1619'),
                ('sgot_ast', 'CIEL', '653'),
                ('sgpt_alt', 'CIEL', '654'),
                ('smear', 'CIEL', ''),
                ('tb_smear', 'PIH', 'TUBERCULOSIS SMEAR RESULT'),
                ('viral_load', 'CIEL', '856'),
                ('western_blot', 'CIEL', '1047'),
                ('white_blood_count', 'CIEL', '678')          
            ''')

        setAutoIncrement("hivmigration_ordered_lab_tests", "(select max(obs_id)+1 from obs)")

        loadFromOracleToMySql('''
            insert into hivmigration_ordered_lab_tests (
              source_encounter_id,
              source_patient_id,
              test,
              test_other
            )
            values(?,?,?,?) 
            ''', '''
            SELECT 
                t.ENCOUNTER_ID as source_encounter_id, 
                e.patient_id as source_patient_id, 
                lower(TRIM(t.test)),
                t.test_other 
            from HIV_ORDERED_LAB_TESTS t, hiv_encounters e, hiv_demographics_real d 
            where t.ENCOUNTER_ID = e.ENCOUNTER_ID and e.patient_id = d.patient_id;
            ''')

        create_tmp_obs_table()
        setAutoIncrement("tmp_obs", "(select max(obs_id)+1 from hivmigration_ordered_lab_tests)")

        executeMysql("Load Ordered Lab Tests observations", ''' 
                                                                                                       
            INSERT INTO tmp_obs (
                source_patient_id, 
                source_encounter_id, 
                concept_uuid,                 
                value_coded_uuid)
            SELECT 
                t.source_patient_id,
                t.source_encounter_id,
                concept_uuid_from_mapping('PIH', 'Lab test ordered coded') as concept_uuid,
                concept_uuid_from_mapping(m.openmrs_concept_source, m.openmrs_concept_code) as value_coded_uuid
            from hivmigration_ordered_lab_tests t, hivmigration_ordered_lab_tests_mapping m
            where t.test is not null and t.test=m.test 
                and (t.test not in ('abdominal_ultrasound', 'chest_xray', 'cxr')) 
                and (m.openmrs_concept_code is not null) and (m.openmrs_concept_code !='');            
        ''')

        executeMysql("Load Other Ordered Lab Tests as Other Non-coded obs comments", ''' 
                                                                                                       
            INSERT INTO tmp_obs (
                source_patient_id, 
                source_encounter_id, 
                concept_uuid,                 
                value_coded_uuid,
                comments)
            SELECT 
                t.source_patient_id,
                t.source_encounter_id,
                concept_uuid_from_mapping('PIH', 'Lab test ordered coded') as concept_uuid,
                concept_uuid_from_mapping('PIH', 'OTHER') as value_coded_uuid,
                group_concat(test_other separator ', ') as comments
            from hivmigration_ordered_lab_tests t
            where t.test like 'other%' 
            group by t.source_encounter_id 
            order by t.source_patient_id;            
        ''')

        executeMysql("Load Radiology Order obs construct", '''
        
            -- Create Radiology report construct obs_group                                                                                           
            INSERT INTO tmp_obs (
                obs_id,
                source_patient_id, 
                source_encounter_id, 
                concept_uuid)
            SELECT 
                t.obs_id,
                t.source_patient_id,
                t.source_encounter_id,
                concept_uuid_from_mapping('PIH', 'Radiology report construct') as concept_uuid
            from hivmigration_ordered_lab_tests t 
            where t.test in ('abdominal_ultrasound', 'chest_xray', 'cxr'); 
            
            -- Create Radiology procedure performed obs
            INSERT INTO tmp_obs (
                obs_group_id,
                source_patient_id, 
                source_encounter_id, 
                concept_uuid,
                value_coded_uuid)
            SELECT 
                t.obs_id,
                t.source_patient_id,
                t.source_encounter_id,
                concept_uuid_from_mapping('PIH', 'Radiology procedure performed') as concept_uuid,
                concept_uuid_from_mapping(m.openmrs_concept_source, m.openmrs_concept_code) as value_coded_uuid
            from hivmigration_ordered_lab_tests t, hivmigration_ordered_lab_tests_mapping m 
            where t.test in ('abdominal_ultrasound', 'chest_xray', 'cxr') 
                and t.test=m.test 
                and (m.openmrs_concept_code is not null) and (m.openmrs_concept_code !=''); 
            
        ''')

        executeMysql("Log warnings for the ordered lab that were not migrated because of missing mapping", ''' 
                                                                                                       
            INSERT INTO hivmigration_data_warnings (
                openmrs_patient_id, 
                openmrs_encounter_id, 
                encounter_date, 
                field_name, 
                field_value, 
                warning_type, 
                flag_for_review)
            SELECT 
                p.person_id as patient_id,
                e.encounter_id as encounter_id,
                e.encounter_date as encounter_date,
                'Ordered Lab Test' as field_name,
                t.test as field_value,
                'No OpenMRS mapping' as warning_type,
                TRUE as flag_for_review
            from hivmigration_ordered_lab_tests t, hivmigration_ordered_lab_tests_mapping m, hivmigration_encounters e, hivmigration_patients p
            where t.test is not null and t.test=m.test and t.source_encounter_id=e.source_encounter_id and e.source_patient_id=p.source_patient_id 
            and ((m.openmrs_concept_code is null) or (m.openmrs_concept_code =''));            
        ''')

        migrate_tmp_obs()
    }

    @Override
    def void revert() {
        executeMysql("drop table if exists hivmigration_ordered_lab_tests_mapping")
        executeMysql("drop table if exists hivmigration_ordered_lab_tests")
    }
}
