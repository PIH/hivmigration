package org.pih.hivmigration.etl.sql

/**
 * Migrates Family Planning section from HIV Follow-up form
 * into Family Planning section of the PIH EMR Follow-up form.
 *
 * Data comes from HIV_OBSERVATIONS / hivmigration_observations.
 */
class FamilyPlanningMigrator extends ObsMigrator {
    @Override
    def void migrate() {

        create_tmp_obs_table()

        executeMysql("Migrate non-grouped family planning obs", '''
            -- sexually active
            INSERT INTO tmp_obs
                (source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT source_encounter_id,
                   concept_uuid_from_mapping('CIEL', '160109'),
                   IF(value = 't',
                      concept_uuid_from_mapping('PIH', 'YES'),
                      concept_uuid_from_mapping('PIH', 'NO'))
            FROM hivmigration_observations
            WHERE observation = 'sexually_active_p' AND value IS NOT NULL;

            -- any FP method checkbox
            INSERT INTO tmp_obs
                (source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT source_encounter_id,
                   concept_uuid_from_mapping('CIEL', '1382'),
                   concept_uuid_from_mapping('PIH', 'YES')
            FROM hivmigration_observations
            WHERE (observation LIKE 'family_planning\\.%' AND value = 't')
                OR observation = 'family_planning_other'
            GROUP BY source_encounter_id;

            -- FP method non-coded
            INSERT INTO tmp_obs
                (source_encounter_id, concept_uuid, value_text)
            SELECT source_encounter_id,
                   concept_uuid_from_mapping('PIH', 'OTHER FAMILY PLANNING METHOD, NON-CODED'),
                   value
            FROM hivmigration_observations
            WHERE observation = 'family_planning_other';

            -- Abstinence as Other
            INSERT INTO tmp_obs
                (source_encounter_id, concept_uuid, value_text)
            SELECT source_encounter_id,
                   concept_uuid_from_mapping('PIH', 'OTHER FAMILY PLANNING METHOD, NON-CODED'),
                   'abstinence'
            FROM hivmigration_observations
            WHERE observation = 'family_planning.abstinence';
        ''')

        migrate_tmp_obs()

        executeMysql("Create temp table for family planning obs groups", '''
            CREATE TABLE hivmigration_tmp_family_planning (
                obs_group_id INT PRIMARY KEY AUTO_INCREMENT,
                source_encounter_id INT,
                method_uuid VARCHAR(38)
            );
        ''')

        setAutoIncrement("hivmigration_tmp_family_planning", "(select max(obs_id)+1 from obs)")

        create_tmp_obs_table()

        executeMysql("Migrate family planning method obs groups", '''

            INSERT INTO hivmigration_tmp_family_planning
                (source_encounter_id, method_uuid)
            SELECT source_encounter_id,
                   concept_uuid_from_mapping('PIH', 'CONDOMS')
            FROM hivmigration_observations
            WHERE observation = 'family_planning.condom' AND value = 't';

            INSERT INTO hivmigration_tmp_family_planning
                (source_encounter_id, method_uuid)
            SELECT source_encounter_id,
                   concept_uuid_from_mapping('PIH', 'NORPLANT')
            FROM hivmigration_observations
            WHERE observation = 'family_planning.norplant' AND value = 't';

            INSERT INTO hivmigration_tmp_family_planning
                (source_encounter_id, method_uuid)
            SELECT source_encounter_id,
                   concept_uuid_from_mapping('PIH', 'ORAL CONTRACEPTION')
            FROM hivmigration_observations
            WHERE observation = 'family_planning.oral_contraceptive' AND value = 't';

            INSERT INTO hivmigration_tmp_family_planning
                (source_encounter_id, method_uuid)
            SELECT source_encounter_id,
                   concept_uuid_from_mapping('PIH', 'OTHER')
            FROM hivmigration_observations
            WHERE observation = 'family_planning_other'
               OR observation = 'family_planning.abstinence';

            -- populate tmp_obs with obs groups from FP method table
            INSERT INTO tmp_obs
                (obs_id, source_encounter_id, concept_uuid)
            SELECT obs_group_id,
                   source_encounter_id,
                   concept_uuid_from_mapping('PIH', 'Family planning construct')
            FROM hivmigration_tmp_family_planning;

            INSERT INTO tmp_obs
                (obs_group_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT obs_group_id,
                   source_encounter_id,
                   concept_uuid_from_mapping('CIEL', '374'),
                   method_uuid
            FROM hivmigration_tmp_family_planning;
        ''')

        migrate_tmp_obs()

        executeMysql("Create temp table for the Planification section of the intake", '''
            CREATE TABLE hivmigration_tmp_planification (
                obs_group_id INT PRIMARY KEY AUTO_INCREMENT,
                source_encounter_id INT,
                method VARCHAR(38),
                comments VARCHAR(96)
            );
        ''')

        setAutoIncrement("hivmigration_tmp_planification", "(select max(obs_id)+1 from obs)")

        executeMysql("Populate hivmigration_tmp_planification table", '''
     
            INSERT INTO hivmigration_tmp_planification(
                source_encounter_id,
                method,
                comments)
            SELECT
                source_encounter_id,
                ordered as method,
                comments
            FROM hivmigration_ordered_other 
            WHERE ordered in ('abstinence', 'oral_contraceptive', 'depo_provera',
            'condom','norplant', 'tubal_ligation', 'vasectomy', 'no_family_planning',
             'family_planning', 'family_planning_other1', 'family_planning_other2');     
                 
        ''')

        create_tmp_obs_table()
        setAutoIncrement("tmp_obs", "(select max(obs_group_id)+1 from hivmigration_tmp_planification)")

        executeMysql("Create family planning obs from the intake planification section", '''
     
            -- Create Family planning construct obs_group
            INSERT INTO tmp_obs(
                obs_id,
                source_encounter_id,
                concept_uuid)
            SELECT
                obs_group_id as obs_id,
                source_encounter_id,
                concept_uuid_from_mapping('PIH', 'Family planning construct') as concept_uuid
            FROM hivmigration_tmp_planification 
            WHERE method in ('abstinence', 'oral_contraceptive', 'depo_provera',
            'condom','norplant', 'tubal_ligation', 'vasectomy', 'no_family_planning',
             'family_planning', 'family_planning_other1', 'family_planning_other2');  
             
             
             -- Create Family planning method obs
            INSERT INTO tmp_obs(
                obs_group_id,
                source_encounter_id,
                concept_uuid,
                value_coded_uuid
                )
            SELECT
                obs_group_id as obs_group_id,
                source_encounter_id,
                concept_uuid_from_mapping('PIH', 'METHOD OF FAMILY PLANNING') as concept_uuid,
                case method 
                    when 'abstinence' then concept_uuid_from_mapping('PIH', 'ABSTINENCE') 
                    when 'oral_contraceptive' then concept_uuid_from_mapping('PIH', 'ORAL CONTRACEPTION') 
                    when 'depo_provera' then concept_uuid_from_mapping('PIH', 'MEDROXYPROGESTERONE ACETATE') 
                    when 'condom' then concept_uuid_from_mapping('PIH', 'CONDOMS') 
                    when 'norplant' then concept_uuid_from_mapping('PIH', 'NORPLANT') 
                    when 'tubal_ligation' then concept_uuid_from_mapping('PIH', 'TUBAL LIGATION') 
                    when 'vasectomy' then concept_uuid_from_mapping('PIH', 'VASECTOMY') 
                    when 'no_family_planning' then concept_uuid_from_mapping('PIH', 'NONE')                                          
                end as value_coded_uuid    
            FROM hivmigration_tmp_planification 
            WHERE method in ('abstinence', 'oral_contraceptive', 'depo_provera',
            'condom','norplant', 'tubal_ligation', 'vasectomy', 'no_family_planning');   
             
             -- Create Family Planning Other methods obs
             INSERT INTO tmp_obs(   
                obs_group_id,             
                source_encounter_id,
                concept_uuid,
                value_coded_uuid)
            SELECT   
                obs_group_id as obs_group_id,
                source_encounter_id,
                concept_uuid_from_mapping('PIH', 'METHOD OF FAMILY PLANNING') as concept_uuid,
                concept_uuid_from_mapping('PIH', 'OTHER') as  value_coded_uuid
            FROM hivmigration_tmp_planification 
            WHERE method in ( 'family_planning', 'family_planning_other1', 'family_planning_other2') 
            group by source_encounter_id;      
             
             -- Create Family Planning Comments obs
             INSERT INTO tmp_obs(                
                source_encounter_id,
                concept_uuid,
                value_text)
            SELECT   
                source_encounter_id,
                concept_uuid_from_mapping('PIH', 'FAMILY PLANNING COMMENT') as concept_uuid,
                group_concat(comments separator '; ') as value_text 
            FROM hivmigration_tmp_planification 
            WHERE method in ( 'family_planning', 'family_planning_other1', 'family_planning_other2') 
                and comments is not null 
            group by source_encounter_id;                       
        ''')

        migrate_tmp_obs()
    }

    @Override
    def void revert() {
        executeMysql("DROP TABLE IF EXISTS hivmigration_tmp_family_planning;")
        executeMysql("DROP TABLE IF EXISTS hivmigration_tmp_planification;")
        clearTable("obs")
    }
}
