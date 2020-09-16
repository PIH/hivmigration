package org.pih.hivmigration.etl.sql

import org.apache.commons.dbutils.handlers.ScalarHandler

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
            and e.PATIENT_ID is not null
            and v.RESULT is not null  -- there are 28 rows with no result
        ''')

        executeMysql("Create encounter_ids reference table", '''
            CREATE TABLE hivmigration_vitals_encounters (
                source_encounter_id int,
                encounter_id int PRIMARY KEY AUTO_INCREMENT,
                KEY `source_encounter_idx` (`source_encounter_id`)
            );
        ''')

        setAutoIncrement("hivmigration_vitals_encounters", "(SELECT max(encounter_id)+1 FROM encounter)")

        executeMysql("Populate encounter_ids reference table", '''
            INSERT INTO hivmigration_vitals_encounters (source_encounter_id)
            SELECT DISTINCT
                source_encounter_id
            FROM hivmigration_vitals;
        ''')

        executeMysql("Create Vitals encounters", '''
            SET @et = (SELECT encounter_type_id FROM encounter_type WHERE uuid = '4fb47712-34a6-40d2-8ed3-e153abbd25b7');
            SET @formId = (SELECT form_id from form WHERE uuid = '68728aa6-4985-11e2-8815-657001b58a90');
            INSERT INTO encounter (encounter_id, encounter_datetime, date_created, encounter_type, form_id, patient_id, creator, location_id, uuid)
            SELECT
                ve.encounter_id,
                e.encounter_date,
                e.date_created,
                @et,
                @formId,
                p.person_id,
                COALESCE(hu.user_id, 1),
                COALESCE(e.location_id, 1),
                uuid()
            FROM hivmigration_vitals v
                JOIN hivmigration_vitals_encounters ve ON v.source_encounter_id = ve.source_encounter_id
                JOIN hivmigration_encounters e ON v.source_encounter_id = e.source_encounter_id
                JOIN hivmigration_patients p ON e.source_patient_id = p.source_patient_id
                JOIN hivmigration_users hu ON e.source_creator_id = hu.source_user_id
            GROUP BY e.source_encounter_id;
        ''')

        executeMysql("Create migrate_tmp_obs variant for vitals migration", '''
            DROP PROCEDURE IF EXISTS migrate_tmp_obs_vitals;
            DELIMITER $$ ;
            CREATE PROCEDURE migrate_tmp_obs_vitals()
            BEGIN
            INSERT INTO obs (
                obs_id, person_id, encounter_id, obs_group_id, obs_datetime, location_id, concept_id,
                value_coded, value_numeric, value_datetime, value_text, creator, date_created, voided, uuid
            )
            SELECT
                o.obs_id, p.person_id, ve.encounter_id, o.obs_group_id, e.encounter_date, e.location_id, q.concept_id,
                a.concept_id, o.value_numeric, o.value_datetime, o.value_text, 1, e.date_created, 0, uuid()
            FROM tmp_obs o
            JOIN       hivmigration_patients p ON o.source_patient_id = p.source_patient_id
            JOIN       hivmigration_encounters e ON o.source_encounter_id = e.source_encounter_id
            JOIN       hivmigration_vitals_encounters ve ON o.source_encounter_id = ve.source_encounter_id
            LEFT JOIN  concept q ON q.uuid = o.concept_uuid
            LEFT JOIN  concept a ON a.uuid = o.value_coded_uuid;
            END $$
            DELIMITER ;
        ''')

        executeMysql("CALL create_tmp_obs_table();")

        setAutoIncrement("tmp_obs", "(SELECT max(obs_id) + 1 FROM obs)")

        executeMysql("Prepare vitals obs for migration", '''
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, value_numeric, concept_uuid)
            SELECT
                source_patient_id,
                source_encounter_id,  -- this is only provided so that the procedure won't choke on the join. We overwrite the encounter_id below.
                -- result unit is unreliable, but fortunately the values are unambiguous
                IF(result < 3.0, round(result * 100, 1), result),
                '3ce93cf2-26fe-102b-80cb-0017a47871b2'
            FROM hivmigration_vitals
            WHERE sign = 'height';

            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, value_numeric, concept_uuid)
            SELECT
                source_patient_id,
                source_encounter_id,
                CASE
                    WHEN result_unit = 'kgs' THEN result
                    WHEN result_unit = 'lbs' THEN round(result * 0.453592, 1)
                    ELSE result  -- use kgs for the 3461 entries with result_unit = NULL
                END,
                '3ce93b62-26fe-102b-80cb-0017a47871b2'
            FROM hivmigration_vitals
            WHERE sign = 'weight';
            
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, value_numeric, concept_uuid)
            SELECT
                source_patient_id,
                source_encounter_id,
                IF(result < 20, result * 10, result),
                '3ce93694-26fe-102b-80cb-0017a47871b2'
            FROM hivmigration_vitals
            WHERE sign = 'blood_pressure_dias';
            
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, value_numeric, concept_uuid)
            SELECT
                source_patient_id,
                source_encounter_id,
                IF(result < 30, result * 10, result),
                '3ce934fa-26fe-102b-80cb-0017a47871b2'
            FROM hivmigration_vitals
            WHERE sign = 'blood_pressure_sys';
            
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, value_numeric, concept_uuid)
            SELECT source_patient_id, source_encounter_id, result, '3ce93824-26fe-102b-80cb-0017a47871b2'
            FROM hivmigration_vitals
            WHERE sign = 'heart_rate';
            
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, value_numeric, concept_uuid)
            SELECT source_patient_id, source_encounter_id, result, '3ceb11f8-26fe-102b-80cb-0017a47871b2'
            FROM hivmigration_vitals
            WHERE sign = 'respiration_rate';
            
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, value_numeric, concept_uuid)
            SELECT 
                source_patient_id,
                source_encounter_id,
                IF(result > 60, round((result - 32.0) * 0.555555, 1), result),
                '3ce939d2-26fe-102b-80cb-0017a47871b2'
            FROM hivmigration_vitals
            WHERE sign = 'temperature';
        ''')

        executeMysql("CALL migrate_tmp_obs_vitals();")
    }

    @Override
    def void revert() {
        // This commented-out code takes a very long time. Use only if needed to preserve other obs.
//        executeMysql("Clear vitals obs", '''
//            SET @c1 = (SELECT concept_id FROM concept WHERE uuid = '3ce93cf2-26fe-102b-80cb-0017a47871b2');
//            SET @c2 = (SELECT concept_id FROM concept WHERE uuid = '3ce93b62-26fe-102b-80cb-0017a47871b2');
//            SET @c3 = (SELECT concept_id FROM concept WHERE uuid = '3ce93694-26fe-102b-80cb-0017a47871b2');
//            SET @c4 = (SELECT concept_id FROM concept WHERE uuid = '3ce934fa-26fe-102b-80cb-0017a47871b2');
//            SET @c5 = (SELECT concept_id FROM concept WHERE uuid = '3ce93824-26fe-102b-80cb-0017a47871b2');
//            SET @c6 = (SELECT concept_id FROM concept WHERE uuid = '3ceb11f8-26fe-102b-80cb-0017a47871b2');
//            SET @c7 = (SELECT concept_id FROM concept WHERE uuid = '3ce939d2-26fe-102b-80cb-0017a47871b2');
//
//            DELETE FROM obs WHERE concept_id IN (@c1, @c2, @c3, @c4, @c5, @c6, @c7);
//        ''')
//        setAutoIncrement("obs", "(select (max(obs_id)+1) from obs)")
        clearTable("obs")
        executeMysql("DROP TABLE IF EXISTS hivmigration_vitals;")
        executeMysql("DROP TABLE IF EXISTS hivmigration_vitals_encounters;")
    }
}
