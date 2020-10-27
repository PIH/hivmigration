package org.pih.hivmigration.etl.sql

class ExamLabResultsMigrator extends ObsMigrator {
    @Override
    def void migrate() {

        executeMysql("Create hivmigration_exam_lab_results temp table", '''
            CREATE TABLE hivmigration_exam_lab_results (
                source_encounter_id INT,
                lab_test varchar(80),
                result varchar(255),
                test_date date
            );
        ''')

        loadFromOracleToMySql('''
            INSERT INTO hivmigration_exam_lab_results (
                source_encounter_id, lab_test, result, test_date
            ) VALUES (?, ?, ?, ?)
        ''',
        '''
            SELECT encounter_id, lab_test, result, test_date 
            FROM HIV_EXAM_LAB_RESULTS
        ''')

        executeMysql("Log warning about missing encounters", '''
            INSERT INTO hivmigration_data_warnings (openmrs_patient_id, openmrs_encounter_id, field_name, field_value, warning_type)
            SELECT
                hp.person_id,
                he.encounter_id,
                CONCAT('HIV_EXAM_LAB_RESULTS ', lab_test),
                helr.result,
                CONCAT(lab_test, ' value belongs to an encounter that was not migrated. Value not migrated.')
            FROM hivmigration_exam_lab_results helr
                     JOIN hivmigration_encounters he on helr.source_encounter_id = he.source_encounter_id
                     JOIN hivmigration_patients hp on he.source_patient_id = hp.source_patient_id
                     LEFT JOIN encounter e on he.encounter_id = e.encounter_id 
            WHERE lab_test IN('cd4', 'hematocrite', 'ppd', 'rpr') AND e.encounter_id IS NULL;
        ''')

        //
        // -------- CD4
        //

        create_tmp_obs_table()

        executeMysql("Migrate CD4", '''
            INSERT INTO tmp_obs (source_encounter_id, concept_uuid, value_numeric)
            SELECT
                he.source_encounter_id,
                concept_uuid_from_mapping('CIEL', '5497'),
                extract_number(result)
            FROM hivmigration_exam_lab_results helr
            JOIN hivmigration_encounters he on helr.source_encounter_id = he.source_encounter_id
            JOIN encounter e on he.encounter_id = e.encounter_id  -- only migrate lab results corresponding to an encounter that was successfully migrated
            WHERE
                lab_test = 'cd4'    
                AND (is_number(result) OR result REGEXP '[0-9]+cell.*')  -- e.g. '500 cell/mm3'
            ;
        ''')

        executeMysql("Log warning about invalid CD4 values", '''
            INSERT INTO hivmigration_data_warnings (openmrs_patient_id, openmrs_encounter_id, field_name, field_value, warning_type)
            SELECT
                hp.person_id,
                he.encounter_id,
                'HIV_EXAM_LAB_RESULTS cd4',
                helr.result,
                'Invalid CD4 value. Not migrated.'
            FROM hivmigration_exam_lab_results helr
            JOIN hivmigration_encounters he on helr.source_encounter_id = he.source_encounter_id
            JOIN hivmigration_patients hp on he.source_patient_id = hp.source_patient_id
            WHERE lab_test = 'cd4' AND NOT is_number(result) AND NOT result REGEXP '^\\d*\\s*mm$';
        ''')

        migrate_tmp_obs()

        //
        // -------- Hematocrit
        //

        create_tmp_obs_table()

        executeMysql("Hematocrit", '''
            INSERT INTO tmp_obs (source_encounter_id, concept_uuid, value_numeric)
            SELECT
                helr.source_encounter_id, 
                concept_uuid_from_mapping('PIH', 'HEMATOCRIT') as concept_uuid,
                extract_number(helr.result)
            FROM hivmigration_exam_lab_results helr
            JOIN hivmigration_encounters he on helr.source_encounter_id = he.source_encounter_id 
            JOIN encounter e on he.encounter_id = e.encounter_id
            where lab_test ='hematocrite' and helr.result is not null and is_number(extract_number(helr.result)); 
        ''')

        executeMysql("Log warning about invalid hematocrite values", '''
            INSERT INTO hivmigration_data_warnings (openmrs_patient_id, openmrs_encounter_id, field_name, field_value, warning_type)
            SELECT                
                hp.person_id,
                he.encounter_id,
                'HIV_EXAM_LAB_RESULTS hematocrite',
                helr.result,
                'Invalid hematocrite value. Not migrated.'
            FROM hivmigration_exam_lab_results helr
            JOIN hivmigration_encounters he on helr.source_encounter_id = he.source_encounter_id 
            JOIN hivmigration_patients hp on he.source_patient_id = hp.source_patient_id 
            where lab_test ='hematocrite' and (helr.result is null or !is_number(extract_number(helr.result)));
        ''')

        migrate_tmp_obs()

        //
        // -------- PPD
        //

        create_tmp_obs_table()

        executeMysql("Migrate coded PPD", '''
            INSERT INTO tmp_obs (source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT
                he.source_encounter_id,
                concept_uuid_from_mapping('PIH', '1435'),
                CASE
                    WHEN result LIKE 'negative%' THEN concept_uuid_from_mapping('PIH', 'NEGATIVE')
                    WHEN result LIKE 'positive%' THEN concept_uuid_from_mapping('PIH', 'POSITIVE')
                    END
            FROM hivmigration_exam_lab_results helr
            JOIN hivmigration_encounters he on helr.source_encounter_id = he.source_encounter_id
            JOIN encounter e on he.encounter_id = e.encounter_id
            WHERE
                lab_test = 'ppd'
                AND result LIKE 'negative%' OR result LIKE 'positive%';
        ''')

        executeMysql("Migrate numeric PPD", '''
            INSERT INTO tmp_obs (source_encounter_id, concept_uuid, value_numeric)
            SELECT
                he.source_encounter_id,
                concept_uuid_from_mapping('PIH', 'TUBERCULIN SKIN TEST'),
                TRIM(result)
            FROM hivmigration_exam_lab_results helr
            JOIN hivmigration_encounters he on helr.source_encounter_id = he.source_encounter_id
            JOIN encounter e on he.encounter_id = e.encounter_id
            WHERE lab_test = 'ppd' AND is_number(result);
        ''')

        executeMysql("Log warning about invalid PPD values", '''
            INSERT INTO hivmigration_data_warnings (openmrs_patient_id, openmrs_encounter_id, field_name, field_value, warning_type)
            SELECT
                hp.person_id,
                he.encounter_id,
                'HIV_EXAM_LAB_RESULTS ppd',
                helr.result,
                'Invalid PPD value. Not migrated.'
            FROM hivmigration_exam_lab_results helr
            JOIN hivmigration_encounters he on helr.source_encounter_id = he.source_encounter_id
            JOIN hivmigration_patients hp on he.source_patient_id = hp.source_patient_id
            WHERE lab_test = 'ppd' AND result NOT LIKE 'negative%' AND result NOT LIKE 'positive%' AND NOT is_number(result);
        ''')

        migrate_tmp_obs()
        
        //
        // -------- RPR
        //

        create_tmp_obs_table()

        executeMysql("Migrate coded rpr", '''
            INSERT INTO tmp_obs (source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT
                he.source_encounter_id,
                concept_uuid_from_mapping('CIEL', '1619'),
                CASE result
                    WHEN 'negative' THEN concept_uuid_from_mapping('PIH', 'NON-REACTIVE')
                    WHEN 'positive' THEN concept_uuid_from_mapping('PIH', 'REACTIVE')
                    END
            FROM hivmigration_exam_lab_results helr
            JOIN hivmigration_encounters he on helr.source_encounter_id = he.source_encounter_id
            JOIN encounter e on he.encounter_id = e.encounter_id
            WHERE
                lab_test = 'rpr'
                AND result IN ('negative', 'positive');
        ''')

        executeMysql("Log warning about invalid rpr values", '''
            INSERT INTO hivmigration_data_warnings (openmrs_patient_id, openmrs_encounter_id, field_name, field_value, warning_type)
            SELECT
                hp.person_id,
                he.encounter_id,
                'HIV_EXAM_LAB_RESULTS rpr',
                helr.result,
                'Invalid RPR value. Not migrated.'
            FROM hivmigration_exam_lab_results helr
            JOIN hivmigration_encounters he on helr.source_encounter_id = he.source_encounter_id
            JOIN hivmigration_patients hp on he.source_patient_id = hp.source_patient_id
            WHERE lab_test = 'rpr' AND result NOT IN ('negative', 'positive');
        ''')

        migrate_tmp_obs()

        //
        // -------- Sputum
        //

        create_tmp_obs_table()

        executeMysql("Migrate Sputum", '''
            INSERT INTO tmp_obs (source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT
                he.source_encounter_id,
                concept_uuid_from_mapping('PIH', 'TUBERCULOSIS SMEAR RESULT'),
                CASE result
                    WHEN 'negative' THEN concept_uuid_from_mapping('PIH', 'NEGATIVE')
                    WHEN 'negatif' THEN concept_uuid_from_mapping('PIH', 'NEGATIVE')
                    WHEN 'positive' THEN concept_uuid_from_mapping('CIEL', '1362') 
                    WHEN '+++' THEN concept_uuid_from_mapping('CIEL', '1364')  
                    WHEN '++' THEN concept_uuid_from_mapping('CIEL', '1363')  
                    WHEN '+' THEN concept_uuid_from_mapping('CIEL', '1362')  
                    END
            FROM hivmigration_exam_lab_results helr
            JOIN hivmigration_encounters he on helr.source_encounter_id = he.source_encounter_id
            JOIN encounter e on he.encounter_id = e.encounter_id
            WHERE
                lab_test = 'sputum'
                AND result IN ('negative', 'negatif', 'positive', '+++', '++', '+');
        ''')

        executeMysql("Log warning about invalid sputum values", '''
            INSERT INTO hivmigration_data_warnings (openmrs_patient_id, openmrs_encounter_id, field_name, field_value, warning_type)
            SELECT
                hp.person_id,
                he.encounter_id,
                'HIV_EXAM_LAB_RESULTS sputum',
                helr.result,
                'Invalid sputum test value. Not migrated.'
            FROM hivmigration_exam_lab_results helr
            JOIN hivmigration_encounters he on helr.source_encounter_id = he.source_encounter_id
            JOIN hivmigration_patients hp on he.source_patient_id = hp.source_patient_id
            WHERE lab_test = 'sputum' AND result NOT IN ('negative', 'negatif', 'positive', '+++', '++', '+');
        ''')

        migrate_tmp_obs()
    }

    @Override
    def void revert() {
        executeMysql("DROP TABLE IF EXISTS hivmigration_exam_lab_results;")
    }
}
