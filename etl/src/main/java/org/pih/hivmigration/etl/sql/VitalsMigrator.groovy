package org.pih.hivmigration.etl.sql

class VitalsMigrator extends SqlMigrator {
    @Override
    def void migrate() {

        executeMysql("Create vitals staging table", '''
            CREATE TABLE hivmigration_vitals (
              source_encounter_id int,
              sign varchar(30),
              result double,
              result_unit varchar(100),
              source_patient_id int,
              KEY `source_encounter_idx` (`source_encounter_id`)
            );
        ''')

        // TODO: should we do something with the 563 vitals with encounter type 'food_study'
        //      or the 3 with type patient_contact?
        loadFromOracleToMySql('''
            INSERT INTO hivmigration_vitals (source_encounter_id, sign, result, result_unit, source_patient_id)
            VALUES (?, ?, ?, ?, ?)
        ''', '''
            select 
                v.ENCOUNTER_ID, 
                v.SIGN, 
                v.RESULT, 
                v.RESULT_UNIT,
                e.PATIENT_ID
            from HIV_EXAM_VITAL_SIGNS v, HIV_ENCOUNTERS e
            where v.ENCOUNTER_ID = e.ENCOUNTER_ID 
            and SIGN in ('height', 'weight', 'bmi')
            and e.TYPE in ('intake', 'followup')
            and e.PATIENT_ID is not null
        ''')

        executeMysql("CALL create_tmp_obs_table();")

        setAutoIncrement("tmp_obs", "(SELECT max(obs_id) + 1 FROM obs)")

        executeMysql("Add vitals obs", '''
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, value_numeric, concept_uuid)
            SELECT
                source_patient_id,
                source_encounter_id,
                CASE
                    -- result unit is unreliable, but fortunately the values are unambiguous
                    WHEN result < 3.0 THEN round(result * 100, 1)
                    WHEN result > 40 AND result < 300 THEN result
                END,
                '3ce93cf2-26fe-102b-80cb-0017a47871b2'
            FROM hivmigration_vitals
            WHERE sign = 'height'
            AND (result < 3.0 OR (result > 40 AND result < 300));

            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, value_numeric, concept_uuid)
            SELECT
                source_patient_id,
                source_encounter_id,
                CASE
                    WHEN result_unit = 'kgs' THEN result
                    WHEN result_unit = 'lbs' THEN round(result * 0.453592, 1)
                    -- Drop 3461 entries with result_unit = NULL
                    -- result distribution for these has Avg 115, stddev 33.5. Not obvious what to use.
                    -- (there are only 813 entries with result_unit not NULL)
                END,
                '3ce93b62-26fe-102b-80cb-0017a47871b2'
            FROM hivmigration_vitals
            WHERE sign = 'weight'
            AND result_unit IN ('kgs', 'lbs');
            
            CALL migrate_tmp_obs();
        ''')
    }

    @Override
    def void revert() {
        executeMysql('''
            DELETE o FROM obs o JOIN concept c ON c.concept_id = o.concept_id
            WHERE c.uuid IN ('3ce93cf2-26fe-102b-80cb-0017a47871b2', '3ce93b62-26fe-102b-80cb-0017a47871b2');
        ''')
        setAutoIncrement("obs", "(select (max(obs_id)+1) from obs)")
        executeMysql("DROP TABLE IF EXISTS hivmigration_vitals;")
    }
}
