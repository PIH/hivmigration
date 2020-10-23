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

        //
        // -------- CD4
        //

        create_tmp_obs_table()

        executeMysql("Migrate CD4", '''
            INSERT INTO tmp_obs (source_encounter_id, concept_uuid, value_numeric)
            SELECT
                he.source_encounter_id,
                concept_uuid_from_mapping('CIEL', '5497'),
                CASE
                    WHEN is_number(result) THEN TRIM(result)
                    WHEN result REGEXP '^\\d*\\s*mm$' THEN extract_number(result)
                    END
            FROM hivmigration_exam_lab_results helr
            JOIN hivmigration_encounters he on helr.source_encounter_id = he.source_encounter_id
            JOIN encounter e on he.encounter_id = e.encounter_id  -- only migrate lab results corresponding to an encounter that was successfully migrated
            WHERE
                lab_test = 'cd4'    
                AND is_number(result) OR result REGEXP '^\\d*\\s*mm$';  -- e.g. '20 mm'
        ''')

        executeMysql("Log warning about invalid CD4 values", '''
            INSERT INTO hivmigration_data_warnings (patient_id, encounter_id, field_name, field_value, note)
            SELECT
                hp.person_id,
                he.encounter_id,
                'HIV_EXAM_LAB_RESULTS cd4',
                helr.result,
                'Invalid value. Not migrated.'
            FROM hivmigration_exam_lab_results helr
            JOIN hivmigration_encounters he on helr.source_encounter_id = he.source_encounter_id
            JOIN hivmigration_patients hp on he.source_patient_id = hp.source_patient_id
            WHERE lab_test = 'cd4' AND NOT is_number(result) AND NOT result REGEXP '^\\d*\\s*mm$';
        ''')

        executeMysql("Log warning about missing encounters", '''
            INSERT INTO hivmigration_data_warnings (patient_id, encounter_id, field_name, field_value, note)
            SELECT
                hp.person_id,
                he.encounter_id,
                'HIV_EXAM_LAB_RESULTS cd4',
                helr.result,
                CONCAT('Belongs to an encounter that was not migrated, source ID ', he.source_encounter_id, '. Not migrated')
            FROM hivmigration_exam_lab_results helr
                     JOIN hivmigration_encounters he on helr.source_encounter_id = he.source_encounter_id
                     JOIN hivmigration_patients hp on he.source_patient_id = hp.source_patient_id
                     LEFT JOIN encounter e on he.encounter_id = e.encounter_id 
            WHERE lab_test = 'cd4' AND e.encounter_id IS NULL;
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
            INSERT INTO hivmigration_data_warnings (patient_id, encounter_id, field_name, field_value, note)
            SELECT
                hp.person_id,
                he.encounter_id,
                'HIV_EXAM_LAB_RESULTS ppd',
                helr.result,
                'Invalid value. Not migrated.'
            FROM hivmigration_exam_lab_results helr
            JOIN hivmigration_encounters he on helr.source_encounter_id = he.source_encounter_id
            JOIN hivmigration_patients hp on he.source_patient_id = hp.source_patient_id
            WHERE lab_test = 'ppd' AND result NOT LIKE 'negative%' AND result NOT LIKE 'positive%' AND NOT is_number(result);
        ''')

        executeMysql("Log warning about missing encounters", '''
            INSERT INTO hivmigration_data_warnings (patient_id, encounter_id, field_name, field_value, note)
            SELECT
                hp.person_id,
                he.encounter_id,
                'HIV_EXAM_LAB_RESULTS ppd',
                helr.result,
                CONCAT('Belongs to an encounter that was not migrated, source ID ', he.source_encounter_id, '. Not migrated')
            FROM hivmigration_exam_lab_results helr
            JOIN hivmigration_encounters he on helr.source_encounter_id = he.source_encounter_id
            JOIN hivmigration_patients hp on he.source_patient_id = hp.source_patient_id
            LEFT JOIN encounter e on he.encounter_id = e.encounter_id
            WHERE lab_test = 'ppd' AND e.encounter_id IS NULL;
        ''')

        migrate_tmp_obs()
    }

    @Override
    def void revert() {
        executeMysql("DROP TABLE IF EXISTS hivmigration_exam_lab_results;")
    }
}
