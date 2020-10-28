package org.pih.hivmigration.etl.sql

class ExamLabResultsMigrator extends ObsMigrator {

    def void migrateLab(String testName,
                        String conceptUuid,
                        String numericValue="NULL",
                        String codedValue="NULL",
                        String whenCondition="TRUE") {

        create_tmp_obs_table()

        executeMysql("Migrate " + testName, '''
            INSERT INTO tmp_obs (source_encounter_id, concept_uuid, value_numeric, value_coded_uuid)
            SELECT
                he.source_encounter_id,
                ''' + conceptUuid + ''',
                ''' + numericValue + ''',
                ''' + codedValue + '''
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
            WHERE lab_test = \'''' + testName + '''\' AND NOT (''' + whenCondition + ''');'''
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
            SELECT r.encounter_id, r.lab_test, r.result, r.test_date 
            FROM HIV_EXAM_LAB_RESULTS r, hiv_encounters e, hiv_demographics_real d 
            WHERE r.encounter_id = e.encounter_id and e.patient_id = d.patient_id
        ''')

        migrateLab("cd4",
                "concept_uuid_from_mapping('CIEL', '5497')",
                "extract_number(result)",
                "NULL",
                "is_number(result) OR result REGEXP '[0-9]+cell.*'"  // e.g. '500 cell/mm3'
        )

        migrateLab("hematocrit",
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

        migrate_tmp_obs()
    }

    @Override
    def void revert() {
        executeMysql("DROP TABLE IF EXISTS hivmigration_exam_lab_results;")
        clearTable("obs")
    }
}
