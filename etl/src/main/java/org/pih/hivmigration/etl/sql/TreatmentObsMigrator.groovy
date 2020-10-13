package org.pih.hivmigration.etl.sql

import com.fasterxml.jackson.module.scala.introspect.ScalaAnnotationIntrospector
import org.apache.commons.dbutils.handlers.ScalarHandler

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
                start_date datetime,
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
                    (name, start_date, cont, stopped, other_value, source_encounter_id, encounter_date)
                SELECT
                   CASE  -- normalize to the names used in the follow-up form
                       WHEN _txName = 'tmp_smx_prophylaxis' THEN 'CTX'
                       WHEN _txName = 'inh_prophylaxis' THEN 'Isoniazid'
                       WHEN _txName LIKE 'fluconazole%' THEN 'Fluconazole'
                       WHEN _txName = 'other_prophylaxis' THEN 'other'
                       END,
                    IF(MAX(base.ordered = _txName), he.encounter_date, NULL) AS start_date,  -- use current date if Initée is checked
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
            CALL load_prophylaxis('fluconazole_low');
            CALL load_prophylaxis('fluconazole_high');
            CALL load_prophylaxis('mosquito_net');
            CALL load_prophylaxis('other_prophylaxis');
            
            DROP PROCEDURE load_prophylaxis;
        ''')

        executeMysql("Load intermediate prophylaxis table from follow-up form", '''
            DROP PROCEDURE IF EXISTS load_follow_up_prophylaxis;
            DELIMITER $$ ;
            CREATE PROCEDURE load_follow_up_prophylaxis(_txName varchar(80))
            BEGIN
                INSERT INTO hivmigration_prophylaxis
                (name, source_encounter_id, encounter_date)
                SELECT
                    _txName,
                    base.source_encounter_id,
                    he.encounter_date
                FROM hivmigration_observations base
                JOIN hivmigration_encounters he ON base.source_encounter_id = he.source_encounter_id
                WHERE base.observation = CONCAT('current_tx.prophylaxis_', _txName)
                GROUP BY base.source_encounter_id;
                
                -- Insert start date. MySQL offers no way to validate dates without throwing an error, so we do
                -- this in two steps. First, add the dates using IGNORE, while attempting to correct any invalid dates.
                -- Then, validate that none of the dates that were added are invalid.
                -- The only kind of invalid dates in the data (at time of writing) are ones for which day values
                -- that exceed the month length. For these, we use the first day of the following month.
                UPDATE IGNORE hivmigration_prophylaxis pro
                JOIN hivmigration_observations start
                ON  pro.source_encounter_id = start.source_encounter_id
                    AND start.observation = CONCAT('current_tx.prophylaxis_', _txName, '_start_date')
                    AND start.value IS NOT NULL
                SET start_date = IF(DAYNAME(start.value) IS NOT NULL,
                                    start.value,
                                    CONCAT(LEFT(start.value, 6), CAST(SUBSTR(start.value, 6, 2) AS UNSIGNED)+1, '-01'))
                WHERE start_date != '0000-00-00 00:00:00';  -- Leave these as NULL
            END $$
            DELIMITER ;
            
            CALL load_follow_up_prophylaxis('CTX');
            CALL load_follow_up_prophylaxis('Fluconazole');
            CALL load_follow_up_prophylaxis('Isoniazid');
            DROP PROCEDURE load_follow_up_prophylaxis;
        ''')

        // Check that there are no invalid dates
        Long numInvalidDates = selectMysql('''
            SELECT count(start_date)
            FROM hivmigration_prophylaxis
            WHERE start_date IS NOT NULL AND DAYNAME(start_date) IS NULL;''',
            new ScalarHandler()
        )
        if (numInvalidDates > 0) {
            throw new Error("Invalid dates were inserted into the hivmigration_prophylaxis table's `start_date` column. Please review the data in that column and fix the logic by which it is inserted.")
        }

        executeMysql("Create obsgroups for prophylaxis prescriptions", '''
            INSERT INTO tmp_obs (obs_id, concept_uuid, source_encounter_id)
            SELECT obs_group_id, concept_uuid_from_mapping('PIH', '10742'), source_encounter_id
            FROM hivmigration_prophylaxis;
        ''')

        executeMysql("Add prophylaxis value (check the checkbox)", '''
            INSERT INTO tmp_obs (source_encounter_id, concept_uuid, value_coded_uuid, obs_group_id)
            SELECT
                   source_encounter_id,
                   concept_uuid_from_mapping('CIEL', '1282'),
                   CASE name
                       WHEN 'CTX' THEN concept_uuid_from_mapping('CIEL', '105281')
                       WHEN 'Isoniazid' THEN concept_uuid_from_mapping('CIEL', '78280')
                       WHEN 'Fluconazole' THEN concept_uuid_from_mapping('CIEL', '76488')
                       END,
                   obs_group_id
            FROM hivmigration_prophylaxis
            WHERE name IN ('CTX', 'Isoniazid', 'Fluconazole');
        ''')

        executeMysql("Add prophylaxis other value", '''
            INSERT INTO tmp_obs (source_encounter_id, concept_uuid, value_coded_uuid, comments, obs_group_id)
            SELECT
                source_encounter_id,
                concept_uuid_from_mapping('CIEL', '1282'),
                concept_uuid_from_mapping('PIH', '3120'),
                other_value,
                obs_group_id
            FROM hivmigration_prophylaxis
            WHERE name = 'other';
        ''')

        // Right now this is just mosquito_net
        executeMysql("Add old prophylaxes", '''
            INSERT INTO tmp_obs (source_encounter_id, concept_uuid, value_coded_uuid, comments, obs_group_id)
            SELECT
                source_encounter_id,
                concept_uuid_from_mapping('CIEL', '1282'),
                concept_uuid_from_mapping('PIH', '3120'),
                name,
                obs_group_id
            FROM hivmigration_prophylaxis
            WHERE name NOT IN ('CTX', 'Isoniazid', 'Fluconazole', 'other');
        ''')

        executeMysql("Add prophylaxis started date", '''
            INSERT INTO tmp_obs (source_encounter_id, concept_uuid, value_datetime, obs_group_id)
            SELECT
                source_encounter_id,
                concept_uuid_from_mapping('CIEL', '163526'),
                start_date,
                obs_group_id
            FROM hivmigration_prophylaxis
            WHERE start_date IS NOT NULL;
        ''')
        // TODO: populate start date for the same prophylaxis on subsequent forms

        // TODO: figure out how to populate end date

        // Mark any prophylaxes with Inite or Continue as "Current Use"
        executeMysql("Add prophylaxis Current Use", '''
            INSERT INTO tmp_obs (source_encounter_id, concept_uuid, value_coded_uuid, obs_group_id)
            SELECT
                source_encounter_id,
                concept_uuid_from_mapping('CIEL', '159367'),
                concept_uuid_from_mapping('CIEL', '1065'),
                obs_group_id
            FROM hivmigration_prophylaxis
            WHERE start_date IS NOT NULL OR cont = 1;
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

        executeMysql("Set up for ARV Regimen migration", '''
            CREATE TABLE hivmigration_arv_regimen (
                obs_group_id int primary key auto_increment,
                source_encounter_id int,
                comments varchar(255)
            );
        ''')
        setAutoIncrement('hivmigration_arv_regimen', '(select max(obs_id)+1 from obs)')

        executeMysql("Populate ARV regimen staging table", '''
            INSERT INTO hivmigration_arv_regimen (source_encounter_id, comments)
            SELECT source_encounter_id, comments
            FROM hivmigration_ordered_other
            WHERE ordered = 'arv_regimen';
            
            INSERT INTO hivmigration_arv_regimen (source_encounter_id, comments)
            SELECT source_encounter_id, comments
            FROM hivmigration_ordered_other
            WHERE ordered = 'arv_regimen_other';
            
            INSERT INTO hivmigration_arv_regimen (source_encounter_id, comments)
            SELECT source_encounter_id, value
            FROM hivmigration_observations
            WHERE observation = 'current_tx.art';
            
            INSERT INTO hivmigration_arv_regimen (source_encounter_id, comments)
            SELECT source_encounter_id, value
            FROM hivmigration_observations
            WHERE observation = 'current_tx.art_other'
                AND LENGTH(TRIM(value)) > 0;  -- skip one weird entry that's 297 chars of whitespace
        ''')

        executeMysql("Create ARV regimen obs group ", '''
            INSERT INTO tmp_obs (source_encounter_id, concept_uuid, obs_id)
            SELECT source_encounter_id, concept_uuid_from_mapping('PIH', '10742'), obs_group_id
            FROM hivmigration_arv_regimen;
        ''')

        executeMysql("Migrate known ARV regimens into coded obs",'''
            INSERT INTO tmp_obs (source_encounter_id, concept_uuid, value_coded_uuid, obs_group_id)
            SELECT
                source_encounter_id,
                concept_uuid_from_mapping('PIH', '6116'),
                CASE
                    WHEN comments LIKE 'azt%3tc%atv/r' THEN concept_uuid_from_mapping('CIEL', '164511')
                    WHEN comments LIKE 'azt%3tc%efv' THEN concept_uuid_from_mapping('CIEL', '160124')
                    WHEN comments LIKE 'azt%3tc%nvp' THEN concept_uuid_from_mapping('CIEL', '1652')
                    WHEN comments LIKE 'd4t%3tc%efv' THEN concept_uuid_from_mapping('CIEL', '160104')
                    WHEN comments LIKE 'd4t%3tc%nvp' THEN concept_uuid_from_mapping('CIEL', '792')
                    WHEN comments LIKE 'tdf%3tc%atv/r' THEN concept_uuid_from_mapping('CIEL', '164512')
                    WHEN comments LIKE 'tdf%3tc%dtg' THEN concept_uuid_from_mapping('CIEL', '165086')
                    WHEN comments LIKE 'tdf%3tc%efv' THEN concept_uuid_from_mapping('CIEL', '164505')
                    WHEN comments LIKE 'tdf%3tc%nvp' THEN concept_uuid_from_mapping('CIEL', '162565')
                    ELSE concept_uuid_from_mapping('CIEL', '5424')  -- other
                    END,
                obs_group_id
            FROM hivmigration_arv_regimen;
        ''')

        executeMysql("Migrate old ARV Regimens into 'other' text box", '''
            INSERT INTO tmp_obs (source_encounter_id, concept_uuid, value_text, obs_group_id)
            SELECT
                source_encounter_id,
                concept_uuid_from_mapping('CIEL', '5424'),
                comments,
                obs_group_id
            FROM hivmigration_arv_regimen
            WHERE comments NOT LIKE 'azt%3tc%atv/r'
              AND comments NOT LIKE 'azt%3tc%efv' 
              AND comments NOT LIKE 'azt%3tc%nvp' 
              AND comments NOT LIKE 'd4t%3tc%efv' 
              AND comments NOT LIKE 'd4t%3tc%nvp' 
              AND comments NOT LIKE 'tdf%3tc%atv/r'
              AND comments NOT LIKE 'tdf%3tc%dtg'
              AND comments NOT LIKE 'tdf%3tc%efv' 
              AND comments NOT LIKE 'tdf%3tc%nvp';
        ''')

        executeMysql("Migrate ART start date from follow-up form", '''
            INSERT IGNORE INTO tmp_obs (source_encounter_id, concept_uuid, value_datetime)
            SELECT
                source_encounter_id,
                concept_uuid_from_mapping('CIEL', '159599'),
                IF(DAYNAME(value) IS NOT NULL,
                   value,
                   CONCAT(LEFT(value, 6), CAST(SUBSTR(value, 6, 2) AS UNSIGNED)+1, '-01'))
            FROM hivmigration_observations
            WHERE observation = 'current_tx.art_start_date';
            
            DELETE FROM tmp_obs
            WHERE value_datetime = '0000-00-00';
        ''')
        // Check that there are no invalid dates
        numInvalidDates = selectMysql('''
                SELECT count(value_datetime)
                FROM tmp_obs
                WHERE value_datetime IS NOT NULL AND DAYNAME(value_datetime) IS NULL;''',
                new ScalarHandler()
        )
        if (numInvalidDates > 0) {
            throw new Error("Invalid dates were inserted into the hivmigration_prophylaxis table's `start_date` column. Please review the data in that column and fix the logic by which it is inserted.")
        }

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

        executeMysql("Migrate TB treatment from follow-up form", '''
            INSERT INTO tmp_obs (source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT
                source_encounter_id,
                concept_uuid_from_mapping('PIH', '6150'),
                CASE
                    WHEN value LIKE 'tb_initial%' OR value LIKE 'tb_infant%' THEN concept_uuid_from_mapping('CIEL', '159792')
                    WHEN value LIKE 'tb_retreatment%%' THEN concept_uuid_from_mapping('CIEL', '159795')
                    WHEN value = 'mdr_tb_treatment' THEN concept_uuid_from_mapping('CIEL', '159909')
                    END
            FROM hivmigration_observations
            WHERE observation = 'current_tx.tb' AND value IS NOT NULL AND value != 'none';
        ''')

        executeMysql("Migrate TB treatment start date", '''
            INSERT INTO tmp_obs (source_encounter_id, concept_uuid, value_datetime)
            SELECT
                source_encounter_id,
                concept_uuid_from_mapping('CIEL', '1113'),
                comments
            FROM hivmigration_ordered_other
            WHERE ordered = 'tb_start_date' AND comments IS NOT NULL;
            
            INSERT INTO tmp_obs (source_encounter_id, concept_uuid, value_datetime)
            SELECT
                source_encounter_id,
                concept_uuid_from_mapping('CIEL', '1113'),
                value
            FROM hivmigration_observations
            WHERE observation = 'current_tx.tb_start_date AND value IS NOT NULL';
        ''')

        executeMysql("Migrate TB regimen", '''
            INSERT INTO tmp_obs (source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT
                source_encounter_id,
                concept_uuid_from_mapping('PIH', '6150'),
                CASE comments
                    WHEN '2HRZE_4HR' THEN concept_uuid_from_mapping('PIH', '2RHEZ / 4RH')  -- 'Initial'
                    WHEN 'mdr_tb_treatment' THEN concept_uuid_from_mapping('CIEL', '159909')  -- 'MDR TB'
                    WHEN 'hrez' THEN concept_uuid_from_mapping('PIH', 'HRZE')
                    WHEN '2S+HRZE_1HRZE_5HR+E' THEN concept_uuid_from_mapping('PIH', '2S+RHEZ / 1RHEZ / 5RH+E')
                END
            FROM hivmigration_ordered_other
            WHERE ordered = 'tb_treatment';
        ''')

        // TODO: migrate 'other' TB treatments
        migrate_tmp_obs()
    }

    @Override
    def void revert() {
        executeMysql("DROP TABLE IF EXISTS hivmigration_ordered_other")
        executeMysql("DROP TABLE IF EXISTS hivmigration_prophylaxis")
        executeMysql("DROP TABLE IF EXISTS hivmigration_arv_regimen")
    }
}
