package org.pih.hivmigration.etl.sql

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
                 concept_uuid_from_mapping('PIH', '1590'),
                 concept_uuid_from_mapping('CIEL', '1282'),
                 concept_uuid_from_mapping('CIEL', '105281')
                    ),
                (
                 'Isoniazid',
                 concept_uuid_from_mapping('PIH', '1590'),
                 concept_uuid_from_mapping('CIEL', '1282'),
                 concept_uuid_from_mapping('CIEL', '78280')
                    ),
                (
                 'other',
                 concept_uuid_from_mapping('PIH', '1501'),
                 concept_uuid_from_mapping('CIEL', '1282'),
                 concept_uuid_from_mapping('CIEL', '5622')
                    ),
                (
                 'depo_provera',
                 concept_uuid_from_mapping('PIH', 'Family planning history construct'),
                 concept_uuid_from_mapping('CIEL', '374'),
                 concept_uuid_from_mapping('CIEL', '907')
                    ),
                (
                 'AZT_3TC_NVP',
                 concept_uuid_from_mapping('PIH', '1501'),
                 concept_uuid_from_mapping('CIEL', '1282'),
                 concept_uuid_from_mapping('PIH', '13014')
                    ),
                (
                 'AZT_3TC_EFV',
                 concept_uuid_from_mapping('PIH', '1501'),
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
                 concept_uuid_from_mapping('PIH', '1501'),
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
                 concept_uuid_from_mapping('PIH', '1501'),
                 concept_uuid_from_mapping('CIEL', '1282'),
                 concept_uuid_from_mapping('PIH', '13012')
                    ),
                (
                 'Fluconazole',
                 concept_uuid_from_mapping('PIH', '1590'),
                 concept_uuid_from_mapping('CIEL', '1282'),
                 concept_uuid_from_mapping('CIEL', '76488')
                    ),
                (
                 'sterilization',
                 concept_uuid_from_mapping('PIH', 'Family planning history construct'),
                 concept_uuid_from_mapping('CIEL', '374'),
                 concept_uuid_from_mapping('CIEL', '1472')
                    )
            ;
        ''')

        executeMysql("Create row-per-group temporary table",'''
            CREATE TABLE hivmigration_tmp_previous_exposures_groups (
                obs_group_id INT PRIMARY KEY AUTO_INCREMENT,
                source_patient_id INT,
                source_encounter_id INT,
                source_value VARCHAR(255),
                start_date DATE,
                end_date DATE
            );
        ''')

        setAutoIncrement("hivmigration_tmp_previous_exposures_groups", "(select max(obs_id)+1 from obs)")

        executeMysql("Migrate previous exposures into row-per-group temporary table",'''
            INSERT INTO hivmigration_tmp_previous_exposures_groups
                (source_patient_id, source_encounter_id, source_value, start_date, end_date)
            SELECT
                hpp.patient_id,
                he.source_encounter_id,
                inn,
                start_date,
                end_date
            FROM hivmigration_previous_exposures hpp
            JOIN (SELECT * FROM hivmigration_encounters WHERE source_encounter_type = 'intake') he
                ON hpp.patient_id = he.source_patient_id
            WHERE inn IS NOT NULL;
        ''')

        create_tmp_obs_table()

        executeMysql("Migrate previous exposures from row-per-group into tmp_obs table",'''
            INSERT INTO tmp_obs
                (obs_id, source_patient_id, source_encounter_id, concept_uuid)
            SELECT
                obs_group_id, source_patient_id, source_encounter_id, map.grouping_concept_uuid
            FROM hivmigration_tmp_previous_exposures_groups pegroups
            JOIN hivmigration_tmp_previous_exposures_map map ON map.inn = pegroups.source_value;
            
            INSERT INTO tmp_obs
                (obs_group_id, source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT
                obs_group_id, source_patient_id, source_encounter_id, map.concept_uuid, map.value_concept_uuid
            FROM hivmigration_tmp_previous_exposures_groups pegroups
            JOIN hivmigration_tmp_previous_exposures_map map ON map.inn = pegroups.source_value;
        ''')

        migrate_tmp_obs()
    }

    @Override
    def void revert() {
        executeMysql('''
            DROP TABLE IF EXISTS hivmigration_tmp_previous_exposures_map;
            DROP TABLE IF EXISTS hivmigration_tmp_previous_exposures_groups;
        ''')
        clearTable("obs")
    }
}
