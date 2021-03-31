package org.pih.hivmigration.etl.sql

class ExamLabResultsMigrator extends ObsMigrator {

    def void migrateLab(String testName,
                        String conceptUuid,
                        String numericValue="NULL",
                        String codedValue="NULL",
                        String whenCondition="TRUE") {

        create_tmp_obs_table()

        executeMysql("Migrate " + testName, '''
            INSERT INTO tmp_obs (source_encounter_id, concept_uuid, value_numeric, value_coded_uuid, obs_datetime)
            SELECT
                he.source_encounter_id,
                ''' + conceptUuid + ''',
                ''' + numericValue + ''',
                ''' + codedValue + ''',
                helr.test_date
            FROM hivmigration_exam_lab_results helr
            JOIN hivmigration_encounters he on helr.source_encounter_id = he.source_encounter_id
            JOIN encounter e on he.encounter_id = e.encounter_id  -- only migrate lab results corresponding to an encounter that was successfully migrated
            WHERE
                lab_test = \'''' + testName + '''\'
                AND (''' + whenCondition + ''');'''
        )

        executeMysql("Log warning about invalid " + testName + " values", '''
            INSERT INTO hivmigration_data_warnings (openmrs_patient_id, openmrs_encounter_id, field_name, field_value, warning_type)
            SELECT
                hp.person_id,
                he.encounter_id,
                \'HIV_EXAM_LAB_RESULTS ''' + testName + '''\',
                result,
                \'Invalid ''' + testName + ''' value. Not migrated.\'
            FROM hivmigration_exam_lab_results helr
            JOIN hivmigration_encounters he ON helr.source_encounter_id = he.source_encounter_id
            JOIN hivmigration_patients hp ON he.source_patient_id = hp.source_patient_id
            WHERE lab_test = \'''' + testName + '''\' AND (NOT (''' + whenCondition + ''') or result is NULL);'''
        )

        executeMysql("Log warning about missing encounters for " + testName, '''
            INSERT INTO hivmigration_data_warnings (openmrs_patient_id, openmrs_encounter_id, field_name, field_value, warning_type, warning_details)
            SELECT
                hp.person_id,
                he.encounter_id,
                'HIV_EXAM_LAB_RESULTS ''' + testName + '''',
                result,
                \'''' + testName + ''' value belongs to an encounter that was not migrated. Value not migrated.',
                CONCAT('Source encounter type ', he.source_encounter_type)
            FROM hivmigration_exam_lab_results helr
                     JOIN hivmigration_encounters he on helr.source_encounter_id = he.source_encounter_id
                     JOIN hivmigration_patients hp on he.source_patient_id = hp.source_patient_id
                     LEFT JOIN encounter e on he.encounter_id = e.encounter_id 
            WHERE lab_test = \'''' + testName + '''' AND e.encounter_id IS NULL;
        ''')


        migrate_tmp_obs()
    }

    def void migrateRadiologyExams(){
        create_tmp_obs_table()
        setAutoIncrement("tmp_obs", "(select max(obs_id)+1 from hivmigration_exam_lab_results)")

        executeMysql("Migrate Radiology orders", '''

            -- Create PIH:Radiology report construct obs_group only for chest x-ray ('cxr0', 'cxr1', 'cxr2')                                                                                          
            INSERT INTO tmp_obs (
                obs_id,                
                source_encounter_id, 
                concept_uuid)
            SELECT 
                obs_id,                
                source_encounter_id,
                concept_uuid_from_mapping('PIH', 'Radiology report construct') as concept_uuid
            from hivmigration_exam_lab_results  
            where lab_test in ('cxr0', 'cxr1', 'cxr2') and (result is not null or test_date is not null)  
            group by source_encounter_id; 
            
            -- Add Radiology chest x-ray procedure performed obs                                                                                           
            INSERT INTO tmp_obs (
                obs_group_id,                
                source_encounter_id, 
                concept_uuid,
                value_coded_uuid)
            SELECT 
                obs_id,                
                source_encounter_id,
                concept_uuid_from_mapping('PIH', 'Radiology procedure performed') as concept_uuid,
                concept_uuid_from_mapping('CIEL', '165152') as value_coded_uuid               
            from hivmigration_exam_lab_results  
            where lab_test in ('cxr0', 'cxr1', 'cxr2') and (result is not null or test_date is not null)  
            group by source_encounter_id; 
            
            -- Add Radiology chest xRay combined findings                                                                                           
            INSERT INTO tmp_obs (
                obs_group_id,                
                source_encounter_id, 
                concept_uuid,
                value_text)
            SELECT 
                obs_id,                
                source_encounter_id,
                concept_uuid_from_mapping('PIH', 'Radiology report comments') as concept_uuid,
                group_concat(concat_ws(': ', lab_test, result, test_date) order by lab_test separator '; ' ) as value_text                 
            from hivmigration_exam_lab_results  
            where lab_test in ('cxr0', 'cxr1', 'cxr2') and (result is not null or test_date is not null)  
            group by source_encounter_id;
            
            -- Add Radiology Date of test obs for chest xRay report                                                                                         
            INSERT INTO tmp_obs (
                obs_group_id,                
                source_encounter_id, 
                concept_uuid,
                value_datetime)
            SELECT 
                obs_id,                
                source_encounter_id,
                concept_uuid_from_mapping('PIH', '12847') as concept_uuid, -- Date of test
                max(test_date) as value_datetime                 
            from hivmigration_exam_lab_results  
            where lab_test in ('cxr0', 'cxr1', 'cxr2') and result is not null and test_date is not null 
            group by source_encounter_id;
            
            -- Create PIH:Radiology report construct obs_group for other radiology exams                                                                                          
            INSERT INTO tmp_obs (
                obs_id,                
                source_encounter_id, 
                concept_uuid)
            SELECT 
                obs_id,                
                source_encounter_id,
                concept_uuid_from_mapping('PIH', 'Radiology report construct') as concept_uuid
            from hivmigration_exam_lab_results  
            where lab_test in ('abdominal_ultrasound', 'other_radiology') and (result is not null or test_date is not null);                          
            
             -- Add Radiology procedure performed obs                                                                                           
            INSERT INTO tmp_obs (
                obs_group_id,                
                source_encounter_id, 
                concept_uuid,
                value_coded_uuid)
            SELECT 
                obs_id,                
                source_encounter_id,
                concept_uuid_from_mapping('PIH', 'Radiology procedure performed') as concept_uuid,
                case 
                    when (lab_test = 'abdominal_ultrasound') then concept_uuid_from_mapping('CIEL', '845') -- Abdominal (U/S)                    
                    else concept_uuid_from_mapping('PIH', 'OTHER NON-CODED')                                     
                end as value_coded_uuid                
            from hivmigration_exam_lab_results  
            where lab_test in ('abdominal_ultrasound', 'other_radiology') and (result is not null or test_date is not null); 
                                    
            -- Add Radiology report findings, except the chest xRay findings                                                                                           
            INSERT INTO tmp_obs (
                obs_group_id,                
                source_encounter_id, 
                concept_uuid,
                value_text)
            SELECT 
                obs_id,                
                source_encounter_id,
                concept_uuid_from_mapping('PIH', 'Radiology report comments') as concept_uuid,
                result as value_text                 
            from hivmigration_exam_lab_results  
            where lab_test in ('abdominal_ultrasound', 'other_radiology') and (result is not null or test_date is not null);
                                    
            -- Add Radiology Date of test obs for all other radiology reports                                                                                         
            INSERT INTO tmp_obs (
                obs_group_id,                
                source_encounter_id, 
                concept_uuid,
                value_datetime)
            SELECT 
                obs_id,                
                source_encounter_id,
                concept_uuid_from_mapping('PIH', '12847') as concept_uuid, -- Date of test
                test_date as value_datetime                 
            from hivmigration_exam_lab_results  
            where lab_test in ('abdominal_ultrasound', 'other_radiology') and result is not null and test_date is not null;
            
        ''')

        migrate_tmp_obs()
    }

    @Override
    def void migrate() {

        executeMysql("Create hivmigration_exam_lab_results temp table", '''
            CREATE TABLE hivmigration_exam_lab_results (
                obs_id int PRIMARY KEY AUTO_INCREMENT,
                source_encounter_id INT,
                lab_test varchar(80),
                result varchar(255),
                test_date date
            );
        ''')
        setAutoIncrement("hivmigration_exam_lab_results", "(select max(obs_id)+1 from obs)")

        loadFromOracleToMySql('''
            INSERT INTO hivmigration_exam_lab_results (
                source_encounter_id, lab_test, result, test_date
            ) VALUES (?, ?, ?, ?)
        ''',
                '''
            SELECT r.encounter_id, r.lab_test, r.result, r.test_date 
            FROM HIV_EXAM_LAB_RESULTS r, hiv_encounters e, hiv_demographics_real d 
            WHERE r.encounter_id = e.encounter_id and e.patient_id = d.patient_id and (result is not null or test_date is not null)
        ''')

        migrateRadiologyExams()

        migrateLab("cd4",
                "concept_uuid_from_mapping('CIEL', '5497')",
                "extract_number(result)",
                "NULL",
                "is_number(result) OR result REGEXP '[0-9]+cell.*'"  // e.g. '500 cell/mm3'
        )

        migrateLab("hematocrite",
                "concept_uuid_from_mapping('PIH', 'HEMATOCRIT')",
                "extract_number(result)",
                "NULL",
                "result is not null and is_number(extract_number(result))"
        )

        migrateLab("ppd",
                '''CASE
                    WHEN result LIKE 'negative%' OR result LIKE 'positive%' THEN concept_uuid_from_mapping('PIH', '1435')
                    WHEN is_number(result) THEN concept_uuid_from_mapping('PIH', 'TUBERCULIN SKIN TEST')
                    END''',
                "IF(is_number(result), extract_number(result), NULL)",
                '''CASE
                    WHEN result LIKE 'negative%' THEN concept_uuid_from_mapping('PIH', 'NEGATIVE')
                    WHEN result LIKE 'positive%' THEN concept_uuid_from_mapping('PIH', 'POSITIVE')
                    END''',
                "result LIKE 'negative%' OR result LIKE 'positive%' OR is_number(result)"
        )

        migrateLab("rpr",
                "concept_uuid_from_mapping('CIEL', '1619')",
                "NULL",
                '''CASE result
                    WHEN 'negative' THEN concept_uuid_from_mapping('PIH', 'NON-REACTIVE')
                    WHEN 'positive' THEN concept_uuid_from_mapping('PIH', 'REACTIVE')
                    END''',
                "result IN ('negative', 'positive')"
        )

        migrateLab("sputum",
                "concept_uuid_from_mapping('PIH', 'TUBERCULOSIS SMEAR RESULT')",
                "NULL",
                '''CASE result
                    WHEN 'negative' THEN concept_uuid_from_mapping('PIH', 'NEGATIVE')
                    WHEN 'negatif' THEN concept_uuid_from_mapping('PIH', 'NEGATIVE')
                    WHEN 'positive' THEN concept_uuid_from_mapping('CIEL', '1362') 
                    WHEN '+++' THEN concept_uuid_from_mapping('CIEL', '1364')  
                    WHEN '++' THEN concept_uuid_from_mapping('CIEL', '1363')  
                    WHEN '+' THEN concept_uuid_from_mapping('CIEL', '1362')  
                    END''',
                "result IN ('negative', 'negatif', 'positive', '+++', '++', '+')"
        )

        migrateLab("hemoglobin",
                "concept_uuid_from_mapping('PIH', 'HEMOGLOBIN')",
                "extract_number(result)",
                "NULL",
                '''result REGEXP '^[[:blank:]]*[0-9\\.]+[[:blank:]]*gr?.dll?$' OR is_number(result)'''
                // e.g. '9.8', '9.8 g/dl', or '9.8gr/dl'
        )

        migrateLab("eryth_sed_rate",
                "concept_uuid_from_mapping('PIH', 'ERYTHROCYTE SEDIMENTATION RATE')",
                "extract_number(result)",
                "NULL",
                "result is not null and is_number(extract_number(result))"
        )
    }

    @Override
    def void revert() {
        executeMysql("DROP TABLE IF EXISTS hivmigration_exam_lab_results;")
        clearTable("obs")
    }
}
