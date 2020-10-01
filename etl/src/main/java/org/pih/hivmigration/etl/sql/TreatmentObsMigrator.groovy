package org.pih.hivmigration.etl.sql

class TreatmentObsMigrator extends ObsMigrator {

    @Override
    def void migrate() {
        // the hivmigration_observations table gets created by ObsLoadingMigrator

        executeMysql("Create staging table for ordered_other", '''
            CREATE TABLE hivmigration_ordered_other (
              ordered_other_id int primary key auto_increment,
              source_encounter_id int,
              ordered varchar(100),
              comments varchar(4000),
              KEY `source_encounter_idx` (`source_encounter_id`)
            );
        ''')

        loadFromOracleToMySql('''
            INSERT INTO hivmigration_ordered_other (source_encounter_id, ordered, comments)
            VALUES (?, ?, ?)
        ''', '''
            SELECT encounter_id, ordered, comments
            FROM hiv_ordered_other
        ''')

        create_tmp_obs_table()

        // doesn't appear in the OpenMRS UI but is meaningful data
        executeMysql("Add Prophylaxis Not Indicated to tmp obs table", '''
            INSERT INTO tmp_obs (source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT
                source_encounter_id,
                concept_uuid_from_mapping('PIH', '2740'),
                concept_uuid_from_mapping('CIEL', '1066')
            FROM hivmigration_observations
            WHERE observation = 'prophylaxis_not_indicated' AND value = 't';
        ''')

        executeMysql("Add Prophylaxis: Yes", '''
            INSERT INTO tmp_obs (source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT
                source_encounter_id,
                concept_uuid_from_mapping('PIH', '2740'),
                concept_uuid_from_mapping('CIEL', '1065')
            FROM hivmigration_ordered_other
            WHERE ordered IN (
                'tmp_smx_prophylaxis', 'inh_prophylaxis', 'fluconazole', 'mosquito_net', 'other_prophylaxis',
                'tmp_smx_prophylaxis_continue', 'inh_prophylaxis_continue', 'fluconazole_continue', 'mosquito_net_continue', 'other_prophylaxis_continue'
            );
        ''')
        // TODO: include old prophylaxes

        //
        // BEGIN prophylaxis group PIH:10742

        executeMysql("Create intermediate table for prophylaxis rows", '''
            CREATE TABLE hivmigration_prophylaxis (
                obs_group_id int primary key auto_increment,
                name varchar(80),
                start boolean,
                cont boolean,
                stopped boolean,
                other_value varchar(80),
                source_encounter_id int,
                encounter_date date
            );
        ''')

        setAutoIncrement('hivmigration_prophylaxis', '(select max(obs_id)+1 from tmp_obs)')

        executeMysql("Load intermediate prophylaxis table", '''
            DROP PROCEDURE IF EXISTS load_prophylaxis;
            DELIMITER $$ ;
            CREATE PROCEDURE load_prophylaxis(_txName varchar(80))
            BEGIN
                INSERT INTO hivmigration_prophylaxis
                    (name, start, cont, stopped, other_value, source_encounter_id, encounter_date)
                SELECT
                    _txName,
                    IF(MAX(base.ordered = _txName), TRUE, FALSE) AS start,
                    IF(MAX(base.ordered = CONCAT(_txName, '_continue')), TRUE, FALSE) AS cont,
                    IF(MAX(base.ordered = CONCAT(_txName, '_stopped')), TRUE, FALSE) AS stopped,
                    other.comments AS other_value,
                    base.source_encounter_id,
                    he.encounter_date
                FROM hivmigration_ordered_other base
                JOIN (
                    SELECT source_encounter_id, comments
                    FROM hivmigration_ordered_other
                    WHERE ordered = 'other_prophylaxis') other ON base.source_encounter_id = other.source_encounter_id
                JOIN hivmigration_encounters he ON base.source_encounter_id = he.source_encounter_id
                WHERE base.ordered IN (_txName, CONCAT(_txName, '_continue'))
                GROUP BY base.source_encounter_id;
            END $$
            DELIMITER ;
            
            CALL load_prophylaxis('tmp_smx_prophylaxis');
            CALL load_prophylaxis('inh_prophylaxis');
            CALL load_prophylaxis('fluconazole');
            CALL load_prophylaxis('other_prophylaxis');
            
            DROP PROCEDURE load_prophylaxis;
        ''')

        executeMysql("Create obsgroups for prophylaxis prescriptions", '''
            INSERT INTO tmp_obs (obs_id, concept_uuid, source_encounter_id)
            SELECT obs_group_id, concept_uuid_from_mapping('PIH', '10742'), source_encounter_id
            FROM hivmigration_prophylaxis;
        ''')

        executeMysql("Add treatment value (check the checkbox)", '''
            INSERT INTO tmp_obs (source_encounter_id, concept_uuid, value_coded_uuid, obs_group_id)
            SELECT
                   source_encounter_id,
                   concept_uuid_from_mapping('CIEL', '1282'),
                   CASE name
                       WHEN 'tmp_smx_prophylaxis' THEN concept_uuid_from_mapping('CIEL', '105281')
                       WHEN 'inh_prophylaxis' THEN concept_uuid_from_mapping('CIEL', '78280')
                       WHEN 'fluconazole' THEN concept_uuid_from_mapping('CIEL', '76488')
                       WHEN 'other_prophylaxis' THEN concept_uuid_from_mapping('PIH', '3120')
                    END,
                   obs_group_id
            FROM hivmigration_prophylaxis;
        ''')

        executeMysql("Add treatment other value", '''
            INSERT INTO tmp_obs (source_encounter_id, concept_uuid, value_text, obs_group_id)
            SELECT
                source_encounter_id,
                concept_uuid_from_mapping('CIEL', '163526'),
                other_value,
                obs_group_id
            FROM hivmigration_prophylaxis
            WHERE name = 'other_prophylaxis';
        ''')
        // TODO: include old prophylaxes

        // Set start date if prophylaxis has Inite
        executeMysql("Add treatment started date", '''
            INSERT INTO tmp_obs (source_encounter_id, concept_uuid, value_datetime, obs_group_id)
            SELECT
                source_encounter_id,
                concept_uuid_from_mapping('CIEL', '163526'),
                encounter_date,
                obs_group_id
            FROM hivmigration_prophylaxis
            WHERE start = 1;
        ''')
        // TODO: populate start date for the same prophylaxis on subsequent forms

        // TODO: figure out how to populate end date

        // Mark any prophylaxes with Inite or Continue as "Current Use"
        executeMysql("Add Current Use", '''
            INSERT INTO tmp_obs (source_encounter_id, concept_uuid, value_coded_uuid, obs_group_id)
            SELECT
                source_encounter_id,
                concept_uuid_from_mapping('CIEL', '159367'),
                concept_uuid_from_mapping('CIEL', '1065'),
                obs_group_id
            FROM hivmigration_prophylaxis
            WHERE start = 1 OR cont = 1;
        ''')

        // TODO: migrate reason stopped

        // END prophylaxis group
        //

        migrate_tmp_obs()
        create_tmp_obs_table()

        executeMysql("Migrate 'Does patient need ART?'", '''
            INSERT INTO tmp_obs (source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT
                source_encounter_id,
                concept_uuid_from_mapping('CIEL', '162703'),
                CASE value
                    WHEN 'no' THEN concept_uuid_from_mapping('CIEL', '1066')
                    WHEN 'yes' THEN concept_uuid_from_mapping('PIH', '1619')  -- yes, immediately
                    WHEN 'yes_after_accomp' THEN concept_uuid_from_mapping('PIH', '2222')
                    WHEN 'yes_continue' THEN concept_uuid_from_mapping('CIEL', '160037')
                    WHEN 'yes_deferred' THEN concept_uuid_from_mapping('PIH', '7262')
                    WHEN 'yes_immediately' THEN concept_uuid_from_mapping('PIH', '1619')
                    WHEN 'yes_ptme' THEN concept_uuid_from_mapping('PIH', '1619')  -- yes, immediately
                    WHEN 'yes_refused' THEN concept_uuid_from_mapping('CIEL', '127750')
                    WHEN 'in_progress' THEN concept_uuid_from_mapping('CIEL', '160037')  -- yes, continue
                END
            FROM hivmigration_observations
            WHERE observation = 'arv_treatment_needed'
                AND value IS NOT NULL;  -- only 2 rows where it is null exist
        ''')

        // TODO: these values are all have source_encounter_type 'food_study'. Figure out what to do with them.
//        executeMysql("Migrate ART Start date", '''
//            INSERT INTO tmp_obs (source_encounter_id, concept_uuid, value_datetime)
//            SELECT
//                source_encounter_id,
//                concept_uuid_from_mapping('CIEL', '159599'),
//                value
//            FROM hivmigration_observations
//            WHERE observation = 'arv_start_date';
//        ''')

        migrate_tmp_obs()
        create_tmp_obs_table()

        // TODO: map the common arv_regimen_other values into the new regimens
        executeMysql("Set up for ARV Regimen migration", '''
            CREATE TABLE hivmigration_arv_regimen (
                obs_group_id int primary key auto_increment,
                source_encounter_id int,
                comments varchar(255)
            );
        ''')
        setAutoIncrement('hivmigration_arv_regimen', '(select max(obs_id)+1 from obs)')

        executeMysql("Migrate ARV Regimen", '''
            INSERT INTO hivmigration_arv_regimen (source_encounter_id, comments)
            SELECT source_encounter_id, comments
            FROM hivmigration_ordered_other
            WHERE ordered = 'arv_regimen';
            
            INSERT INTO tmp_obs (source_encounter_id, concept_uuid, obs_id)
            SELECT source_encounter_id, concept_uuid_from_mapping('PIH', '10742'), obs_group_id
            FROM hivmigration_arv_regimen;

            INSERT INTO tmp_obs (source_encounter_id, concept_uuid, value_coded_uuid, obs_group_id)
            SELECT
                source_encounter_id,
                concept_uuid_from_mapping('CIEL', '1282'),
                CASE comments
                    WHEN 'azt_3tc_efv' THEN concept_uuid_from_mapping('CIEL', '160124')
                    WHEN 'azt_3tc_nvp' THEN concept_uuid_from_mapping('CIEL', '1652')
                    WHEN 'd4t_3tc_efv' THEN concept_uuid_from_mapping('CIEL', '160104')
                    WHEN 'd4t_3tc_nvp' THEN concept_uuid_from_mapping('CIEL', '792')
                    ELSE concept_uuid_from_mapping('CIEL', '5424')  -- other
                    END,
                obs_group_id
            FROM hivmigration_arv_regimen;
        ''')

        migrate_tmp_obs()
        create_tmp_obs_table()

        executeMysql("Migrate old ARV Regimens into other text box", '''
            INSERT INTO tmp_obs (source_encounter_id, concept_uuid, value_text)
            SELECT
                source_encounter_id,
                concept_uuid_from_mapping('CIEL', '5424'),
                comments
            FROM hivmigration_ordered_other
            WHERE ordered = 'arv_regimen'
                AND comments NOT IN ('azt_3tc_efv', 'azt_3tc_nvp', 'd4t_3tc_efv', 'd4t_3tc_nvp', 'other');
        ''')

        executeMysql("Migrate ARV Other text", '''
            INSERT INTO tmp_obs (source_encounter_id, concept_uuid, value_text)
            SELECT
                source_encounter_id,
                concept_uuid_from_mapping('CIEL', '5424'),
                comments
            FROM hivmigration_ordered_other
            WHERE ordered = 'arv_regimen_other';
        ''')

        migrate_tmp_obs()
        create_tmp_obs_table()

        executeMysql("Migrate TB Treatment Needed", '''
            INSERT INTO tmp_obs (source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT
                source_encounter_id,
                concept_uuid_from_mapping('PIH', '1559'),
                CASE value
                    WHEN 'prior_not_indicated' THEN concept_uuid_from_mapping('CIEL', '1066')  -- no
                    WHEN 'no' THEN concept_uuid_from_mapping('CIEL', '1066')
                    WHEN 'exam_in_progress' THEN concept_uuid_from_mapping('PIH', '2224')
                    WHEN 'stop_tx' THEN concept_uuid_from_mapping('CIEL', '1066')  -- no
                    WHEN 'start_tx' THEN concept_uuid_from_mapping('CIEL', '1065')  -- yes
                    WHEN 'yes' THEN concept_uuid_from_mapping('CIEL', '1065')
                    WHEN 'already_in_treatment' THEN concept_uuid_from_mapping('PIH', '1432')
                    WHEN 'change_tx' THEN concept_uuid_from_mapping('CIEL', '1065')  -- yes
                END
            FROM hivmigration_observations
            WHERE observation = 'tb_treatment_needed' AND value IS NOT NULL;
        ''')

        executeMysql("Migrate TB treatment start date", '''
            INSERT INTO tmp_obs (source_encounter_id, concept_uuid, value_datetime)
            SELECT
                source_encounter_id,
                concept_uuid_from_mapping('CIEL', '1113'),
                comments
            FROM hivmigration_ordered_other
            WHERE ordered = 'tb_start_date';
        ''')

        // TODO: map 'hrez', '2S+HRZE_1HRZE_5HR+E'
        executeMysql("Migrate TB regimen", '''
            INSERT INTO tmp_obs (source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT
                source_encounter_id,
                concept_uuid_from_mapping('PIH', '6150'),
                CASE comments
                    WHEN '2HRZE_4HR' THEN concept_uuid_from_mapping('PIH', '2125')  -- 'Initial'
                    WHEN 'mdr_tb_treatment' THEN concept_uuid_from_mapping('CIEL', '159909')  -- 'MDR TB'
                END
            FROM hivmigration_ordered_other
            WHERE ordered = 'tb_treatment';
        ''')

        migrate_tmp_obs()
    }

    @Override
    def void revert() {
        executeMysql("DROP TABLE IF EXISTS hivmigration_ordered_other")
        executeMysql("DROP TABLE IF EXISTS hivmigration_prophylaxis")
        executeMysql("DROP TABLE IF EXISTS hivmigration_arv_regimen")
    }
}
