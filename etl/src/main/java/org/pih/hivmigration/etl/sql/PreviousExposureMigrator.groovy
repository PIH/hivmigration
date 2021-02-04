package org.pih.hivmigration.etl.sql

/**
 * Migrates from Intake form sections 6 and 7 to "History of therapeutics" section of the new intake form.
 *
 * Data comes from HIV_PREVIOUS_EXPOSURES, which is migrated by StagingTablesMigrator.
 *
 * https://pihemr.atlassian.net/browse/UHM-5039
 */
class PreviousExposureMigrator extends ObsMigrator {

    @Override
    def void migrate() {

        executeMysql("Create previous exposures mapping table",'''
            CREATE TABLE hivmigration_tmp_previous_exposures_map (
                inn VARCHAR(255),
                grouping_concept_uuid CHAR(38),
                concept_uuid CHAR(38),
                value_concept_uuid CHAR(38)
            );
        
            INSERT INTO hivmigration_tmp_previous_exposures_map
            VALUES
                (
                 'CTX',
                 concept_uuid_from_mapping('PIH', 'PREVIOUS TREATMENT PROPHYLAXIS CONSTRUCT'),
                 concept_uuid_from_mapping('CIEL', '1282'),
                 concept_uuid_from_mapping('CIEL', '105281')
                    ),
                (
                 'Isoniazid',
                 concept_uuid_from_mapping('PIH', 'PREVIOUS TREATMENT PROPHYLAXIS CONSTRUCT'),
                 concept_uuid_from_mapping('CIEL', '1282'),
                 concept_uuid_from_mapping('CIEL', '78280')
                    ),
                (
                 'depo_provera',
                 concept_uuid_from_mapping('PIH', 'Family planning history construct'),
                 concept_uuid_from_mapping('CIEL', '374'),
                 concept_uuid_from_mapping('CIEL', '907')
                    ),
                (
                 'AZT_3TC_NVP',
                 concept_uuid_from_mapping('PIH', 'PREVIOUS TREATMENT HISTORY HIV CONSTRUCT'),
                 concept_uuid_from_mapping('CIEL', '1282'),
                 concept_uuid_from_mapping('PIH', '13014')
                    ),
                (
                 'AZT_3TC_EFV',
                 concept_uuid_from_mapping('PIH', 'PREVIOUS TREATMENT HISTORY HIV CONSTRUCT'),
                 concept_uuid_from_mapping('CIEL', '1282'),
                 concept_uuid_from_mapping('PIH', '13014')
                    ),
                (
                 'condom',
                 concept_uuid_from_mapping('PIH', 'Family planning history construct'),
                 concept_uuid_from_mapping('CIEL', '374'),
                 concept_uuid_from_mapping('CIEL', '190')
                    ),
                (
                 'oral_contraceptive',
                 concept_uuid_from_mapping('PIH', 'Family planning history construct'),
                 concept_uuid_from_mapping('CIEL', '374'),
                 concept_uuid_from_mapping('CIEL', '780')
                    ),
                (
                 'D4T_3TC_NVP',
                 concept_uuid_from_mapping('PIH', 'PREVIOUS TREATMENT HISTORY HIV CONSTRUCT'),
                 concept_uuid_from_mapping('CIEL', '1282'),
                 concept_uuid_from_mapping('PIH', '13012')
                    ),
                (
                 'norplant',
                 concept_uuid_from_mapping('PIH', 'Family planning history construct'),
                 concept_uuid_from_mapping('CIEL', '374'),
                 concept_uuid_from_mapping('CIEL', '78796')
                    ),
                (
                 'D4T_3TC_EFV',
                 concept_uuid_from_mapping('PIH', 'PREVIOUS TREATMENT HISTORY HIV CONSTRUCT'),
                 concept_uuid_from_mapping('CIEL', '1282'),
                 concept_uuid_from_mapping('PIH', '13012')
                    ),
                (
                 'Fluconazole',
                 concept_uuid_from_mapping('PIH', 'PREVIOUS TREATMENT PROPHYLAXIS CONSTRUCT'),
                 concept_uuid_from_mapping('CIEL', '1282'),
                 concept_uuid_from_mapping('CIEL', '76488')
                    ),
                (
                 'sterilization',
                 concept_uuid_from_mapping('PIH', 'Family planning history construct'),
                 concept_uuid_from_mapping('CIEL', '374'),
                 concept_uuid_from_mapping('CIEL', '1472')
                    ),
                (
                 '2HRZE_4HR',
                 concept_uuid_from_mapping('PIH', 'PREVIOUS TB TREATMENT HISTORY CONSTRUCT'),
                 concept_uuid_from_mapping('CIEL', '1190'),
                 concept_uuid_from_mapping('PIH', '2406')
                    ),
                (
                 '2S+HRZE_1HRZE_5HR+E',
                 concept_uuid_from_mapping('PIH', 'PREVIOUS TB TREATMENT HISTORY CONSTRUCT'),
                 concept_uuid_from_mapping('CIEL', '1190'),
                 concept_uuid_from_mapping('PIH', '2S+RHEZ / 1RHEZ / 5RH+E')
                    ),
                (
                 'mdr_tb_treatment',
                 concept_uuid_from_mapping('PIH', 'PREVIOUS TB TREATMENT HISTORY CONSTRUCT'),
                 concept_uuid_from_mapping('CIEL', '1190'),
                 concept_uuid_from_mapping('CIEL', '159909')
                    ),
                -- Hypothetical future values:
                -- These don't exist in the data presently, but it is possible for the latest forms to
                -- create these kinds of values.
                (
                 'AZT_3TC_EFV_NVP',
                 concept_uuid_from_mapping('PIH', 'PREVIOUS TREATMENT HISTORY HIV CONSTRUCT'),
                 concept_uuid_from_mapping('CIEL', '1282'),
                 concept_uuid_from_mapping('PIH', '13014')
                    ),
                (
                 'TDF_3TC_LPV_r',
                 concept_uuid_from_mapping('PIH', 'PREVIOUS TREATMENT HISTORY HIV CONSTRUCT'),
                 concept_uuid_from_mapping('CIEL', '1282'),
                 concept_uuid_from_mapping('PIH', 'TENOFOVIR LAMIVUDINE LOPINAVIR RITONAVIR')
                    ),
                (
                 'TDF_3TC_EFV_NVP',
                 concept_uuid_from_mapping('CIEL', 'PREVIOUS TREATMENT HISTORY HIV CONSTRUCT'),
                 concept_uuid_from_mapping('CIEL', '1282'),
                 concept_uuid_from_mapping('CIEL', '166052')
                    ),
                (
                 '2HRZ_4HR',
                 concept_uuid_from_mapping('PIH', 'PREVIOUS TB TREATMENT HISTORY CONSTRUCT'),
                 concept_uuid_from_mapping('CIEL', '1190'),
                 concept_uuid_from_mapping('PIH', '2RHZ / 4RH')
                    )
            ;
        ''')

        executeMysql("Create row-per-group temporary table",'''
            CREATE TABLE hivmigration_tmp_previous_exposures_groups (
                obs_group_id INT PRIMARY KEY AUTO_INCREMENT,
                source_encounter_id INT,
                grouping_concept_uuid VARCHAR(38),
                concept_uuid VARCHAR(38),
                value_concept_uuid VARCHAR(38),  -- value for concept_uuid
                other_concept_uuid VARCHAR(38),
                other_value VARCHAR(256),  -- value for other_concept_uuid
                start_date DATE,
                end_date DATE
            );
        ''')

        setAutoIncrement("hivmigration_tmp_previous_exposures_groups", "(select max(obs_id)+1 from obs)")

        executeMysql("Migrate previous exposures into row-per-group temporary table",'''
            INSERT INTO hivmigration_tmp_previous_exposures_groups
                (source_encounter_id, grouping_concept_uuid, concept_uuid, value_concept_uuid, start_date, end_date)
            SELECT
                he.source_encounter_id,
                map.grouping_concept_uuid,
                map.concept_uuid,
                map.value_concept_uuid,
                start_date,
                end_date
            FROM hivmigration_previous_exposures hpp
            JOIN (SELECT * FROM hivmigration_encounters WHERE source_encounter_type = 'intake') he
                ON hpp.source_patient_id = he.source_patient_id
            JOIN hivmigration_tmp_previous_exposures_map map ON map.inn = hpp.inn;
            
            -- When inn is null and treatment_other is non-null, migrate to family planning other
            INSERT INTO hivmigration_tmp_previous_exposures_groups
                (source_encounter_id, grouping_concept_uuid, concept_uuid, value_concept_uuid, other_concept_uuid, other_value, start_date, end_date)
            SELECT
                he.source_encounter_id,
                concept_uuid_from_mapping('PIH', 'Family planning history construct'),
                concept_uuid_from_mapping('CIEL', '374'),
                concept_uuid_from_mapping('CIEL', '5622'),  -- Other non-coded
                concept_uuid_from_mapping('PIH', '2996'),
                treatment_other,
                start_date,
                end_date
            FROM hivmigration_previous_exposures hpp
            JOIN (SELECT * FROM hivmigration_encounters WHERE source_encounter_type = 'intake') he
                ON hpp.source_patient_id = he.source_patient_id
            WHERE hpp.inn IS NULL;
            
            -- DMPA in treatment_other gets migrated as coded depo_provera
            INSERT INTO hivmigration_tmp_previous_exposures_groups
                (source_encounter_id, grouping_concept_uuid, concept_uuid, value_concept_uuid, start_date, end_date)
            SELECT
                he.source_encounter_id,
                concept_uuid_from_mapping('PIH', 'Family planning history construct'),
                concept_uuid_from_mapping('CIEL', '374'),
                concept_uuid_from_mapping('CIEL', '79494'),
                start_date,
                end_date
            FROM hivmigration_previous_exposures hpp
            JOIN (SELECT * FROM hivmigration_encounters WHERE source_encounter_type = 'intake') he
                ON hpp.source_patient_id = he.source_patient_id
            WHERE hpp.treatment_other = 'DMPA';
        ''')

        executeMysql("Create obs groups for concatenated HIV Other values", '''
            -- We extract unmapped values from inn, and values of treatment_other that are
            CREATE TABLE hivmigration_tmp_previous_exposures_inn_other (
                source_encounter_id INT PRIMARY KEY,
                inn_other_text VARCHAR(256)
            );
            
            INSERT INTO hivmigration_tmp_previous_exposures_inn_other
                (source_encounter_id, inn_other_text)
            SELECT source_encounter_id,
                   GROUP_CONCAT(
                           CONCAT(hpp.inn,
                                  IF(start_date IS NOT NULL OR end_date IS NOT NULL,
                                     CONCAT(' (',
                                            IF(start_date IS NOT NULL, CONCAT('du ', TRIM(start_date), ' '), ''),
                                            IF(end_date IS NOT NULL, CONCAT('à ', TRIM(end_date)), ''),
                                            ')'),
                                     ''))
                           SEPARATOR ', ') AS inn_other_text
            FROM hivmigration_previous_exposures hpp
            LEFT JOIN hivmigration_tmp_previous_exposures_map map ON hpp.inn = map.inn  -- finding inn with no match in the map
            JOIN (SELECT * FROM hivmigration_encounters WHERE source_encounter_type = 'intake') he
                ON hpp.source_patient_id = he.source_patient_id
            WHERE hpp.inn IS NOT NULL AND hpp.inn != 'other' AND hpp.treatment_other IS NULL AND map.inn IS NULL
            GROUP BY source_encounter_id;
            
            CREATE TABLE hivmigration_tmp_previous_exposures_tx_other (
                source_encounter_id INT PRIMARY KEY,
                tx_other_text VARCHAR(256)
            );
            
            INSERT INTO hivmigration_tmp_previous_exposures_tx_other
                (source_encounter_id, tx_other_text)
            SELECT source_encounter_id,
                   GROUP_CONCAT(
                           CONCAT(hpp.treatment_other,
                                  IF(start_date IS NOT NULL OR end_date IS NOT NULL,
                                     CONCAT(' (',
                                            IF(start_date IS NOT NULL, CONCAT('du ', TRIM(start_date), ' '), ''),
                                            IF(end_date IS NOT NULL, CONCAT('à ', TRIM(end_date)), ''),
                                            ')'),
                                     ''))
                           SEPARATOR ', ') AS tx_other_text
            FROM hivmigration_previous_exposures hpp
            JOIN (SELECT * FROM hivmigration_encounters WHERE source_encounter_type = 'intake') he
                ON hpp.source_patient_id = he.source_patient_id
            WHERE hpp.inn IS NOT NULL AND hpp.treatment_other IS NOT NULL AND hpp.treatment_other != 'DMPA\'
            GROUP BY source_encounter_id;
            
            INSERT INTO hivmigration_tmp_previous_exposures_groups
            (source_encounter_id, grouping_concept_uuid, concept_uuid, value_concept_uuid, other_concept_uuid, other_value)
            SELECT
                source_encounter_id,
                concept_uuid_from_mapping('PIH', 'PREVIOUS TREATMENT HISTORY HIV CONSTRUCT'),
                concept_uuid_from_mapping('CIEL', '1282'),  -- Medication orders
                concept_uuid_from_mapping('CIEL', '5622'),  -- Other non-coded
                concept_uuid_from_mapping('PIH', '1527'),  -- Previous treatment with other medications, non-coded
                CONCAT(inn_other_text,
                       IF(inn_other_text != '' AND tx_other_text != '', ', ', ''),
                       tx_other_text)
            FROM (
                 SELECT inn_other.source_encounter_id, inn_other_text, '' AS tx_other_text
                 FROM hivmigration_tmp_previous_exposures_inn_other inn_other
                          LEFT JOIN hivmigration_tmp_previous_exposures_tx_other tx_other
                                    on inn_other.source_encounter_id = tx_other.source_encounter_id
                 UNION ALL
                 SELECT inn_other.source_encounter_id, '' AS inn_other_text, tx_other_text
                 FROM hivmigration_tmp_previous_exposures_inn_other inn_other
                          RIGHT JOIN hivmigration_tmp_previous_exposures_tx_other tx_other
                                     on inn_other.source_encounter_id = tx_other.source_encounter_id
                 WHERE inn_other.source_encounter_id IS NULL) other_texts;
        ''')

        create_tmp_obs_table()

        executeMysql("Migrate previous exposures from row-per-group into tmp_obs table",'''
            INSERT INTO tmp_obs
                (obs_id, source_encounter_id, concept_uuid)
            SELECT
                obs_group_id, source_encounter_id, grouping_concept_uuid
            FROM hivmigration_tmp_previous_exposures_groups;
            
            INSERT INTO tmp_obs
                (obs_group_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT
                obs_group_id, source_encounter_id, concept_uuid, value_concept_uuid
            FROM hivmigration_tmp_previous_exposures_groups
            WHERE value_concept_uuid IS NOT NULL;
            
            -- "Specify, if other" value is not part of the obs group
            INSERT INTO tmp_obs
                (source_encounter_id, concept_uuid, value_text)
            SELECT
                source_encounter_id, other_concept_uuid, other_value
            FROM hivmigration_tmp_previous_exposures_groups
            WHERE other_value IS NOT NULL;
            
            INSERT INTO hivmigration_data_warnings
                (openmrs_patient_id, field_name, field_value, warning_type, warning_details)
            SELECT (SELECT person_id FROM hivmigration_patients hp WHERE hp.source_patient_id = hpe.source_patient_id),
                   'Previous exposure start and end date',
                   CONCAT(start_date, ' vs ', end_date),
                   'Previous exposure start date is after end date',
                   CONCAT('inn: ', inn, '; treatment_other: ', treatment_other)
            FROM hivmigration_previous_exposures hpe
            WHERE end_date < start_date;
            
            INSERT INTO tmp_obs
                (obs_group_id, source_encounter_id, concept_uuid, value_datetime)
            SELECT
                obs_group_id, source_encounter_id, concept_uuid_from_mapping('CIEL', '1190'), start_date
            FROM hivmigration_tmp_previous_exposures_groups
            WHERE start_date IS NOT NULL;
            
            INSERT INTO tmp_obs
                (obs_group_id, source_encounter_id, concept_uuid, value_datetime)
            SELECT
                obs_group_id, source_encounter_id, concept_uuid_from_mapping('CIEL', '1191'), end_date
            FROM hivmigration_tmp_previous_exposures_groups
            WHERE end_date IS NOT NULL;
        ''')

        migrate_tmp_obs()
    }

    @Override
    def void revert() {
        executeMysql('''
            DROP TABLE IF EXISTS hivmigration_tmp_previous_exposures_map;
            DROP TABLE IF EXISTS hivmigration_tmp_previous_exposures_groups;
            DROP TABLE IF EXISTS hivmigration_tmp_previous_exposures_inn_other;
            DROP TABLE IF EXISTS hivmigration_tmp_previous_exposures_tx_other;
        ''')
        clearTable("obs")
    }
}
