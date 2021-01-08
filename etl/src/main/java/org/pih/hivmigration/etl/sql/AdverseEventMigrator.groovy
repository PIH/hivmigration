package org.pih.hivmigration.etl.sql

class AdverseEventMigrator  extends ObsMigrator {

    @Override
    def void migrate() {
        executeMysql("Create staging table to migrate adverse events", '''
            create table hivmigration_tmp_adverse_event (
              obs_group_id int PRIMARY KEY AUTO_INCREMENT,
              source_encounter_id int,
              reason VARCHAR(64),
              adverse_effect VARCHAR(16),
              adverse_effect_date date
            );
        ''')

        setAutoIncrement("hivmigration_tmp_adverse_event", "(select max(obs_id)+1 from obs)")

        loadFromOracleToMySql('''
            insert into hivmigration_tmp_adverse_event
               (reason,                                
                adverse_effect,
                adverse_effect_date)
            values (?, ?, ?)
        ''', '''
            select 
                case when (a.INN is not null) then a.INN else a.OTHER_REASON end as reason, 
                a.TYPE_OF_REACTION, 
                to_char(a.ALLERGY_DATE, 'yyyy-mm-dd') as adverse_effect_date  
            from HIV_ALLERGIES a
        ''')
        // Add Intake source_encounter_id
        executeMysql('''
            update hivmigration_tmp_adverse_event a, hivmigration_encounters e 
            set a.source_encounter_id = e.source_encounter_id 
            where e.source_encounter_type = 'intake' ;
        ''')

        executeMysql("Create staging table to migrate adverse events Yes checkboxes", '''
            create table hivmigration_tmp_adverse_event_yes (
              obs_group_id int PRIMARY KEY AUTO_INCREMENT,
              source_encounter_id int
            );
        ''')
        setAutoIncrement("hivmigration_tmp_adverse_event_yes", "(select max(obs_group_id)+1 from hivmigration_tmp_adverse_event)")
        executeMysql("Load hivmigration_tmp_adverse_event_yes staging table",
                '''
                insert into hivmigration_tmp_adverse_event_yes(
                    source_encounter_id)
                select 
                    distinct source_encounter_id
                from  hivmigration_tmp_adverse_event;       
        ''')

        create_tmp_obs_table()
        setAutoIncrement("tmp_obs", "(select max(obs_group_id)+1 from hivmigration_tmp_adverse_event_yes)")

        executeMysql("Load adverse events as observations",
                '''
            -- Adverse effect construct
            INSERT INTO tmp_obs (obs_id, source_encounter_id, concept_uuid)
            SELECT
                obs_group_id,                     
                source_encounter_id,
                concept_uuid_from_mapping('PIH', 'ADVERSE EFFECT CONSTRUCT')
            FROM hivmigration_tmp_adverse_event; 

            -- Reason for the Adverse effect
            INSERT INTO tmp_obs (
                obs_group_id, 
                value_text, 
                source_encounter_id, 
                concept_uuid)
            SELECT  
                obs_group_id,
                reason,
                source_encounter_id,
                concept_uuid_from_mapping('PIH', 'GENERAL FREE TEXT')
            FROM hivmigration_tmp_adverse_event 
            WHERE reason is not null; 
            
            -- Adverse effect
            INSERT INTO tmp_obs (
                obs_group_id, 
                value_coded_uuid, 
                source_encounter_id, 
                concept_uuid, 
                comments)
            SELECT  
                obs_group_id,
                CASE adverse_effect 
                    WHEN 'Rash' THEN concept_uuid_from_mapping('CIEL', '512') 
                    WHEN 'Neuropathy' THEN concept_uuid_from_mapping('CIEL', '118983')
                    WHEN 'Aniphylaxis' THEN concept_uuid_from_mapping('CIEL', '148888')
                    ELSE concept_uuid_from_mapping('CIEL', '5622') 
                END value_coded_uuid,                        
                source_encounter_id,
                concept_uuid_from_mapping('PIH', 'ADVERSE EFFECT') as concept_uuid,
                IF(adverse_effect not in ('Rash', 'Neuropathy', 'Aniphylaxis'), adverse_effect, NULL) as comments
            FROM hivmigration_tmp_adverse_event 
            WHERE adverse_effect is not null; 
            
            -- Adverse effect date
            INSERT INTO tmp_obs (
                obs_group_id, 
                value_datetime, 
                source_encounter_id, 
                concept_uuid)
            SELECT  
                obs_group_id,
                adverse_effect_date,
                source_encounter_id,
                concept_uuid_from_mapping('PIH', 'ADVERSE EFFECT DATE')
            FROM hivmigration_tmp_adverse_event 
            WHERE adverse_effect_date is not null; 
            
            
            -- Adverse effect construct for the "Adverse event Yes" checkbox
            INSERT INTO tmp_obs (obs_id, source_encounter_id, concept_uuid)
            SELECT
                obs_group_id,                     
                source_encounter_id,
                concept_uuid_from_mapping('PIH', 'ADVERSE EFFECT CONSTRUCT')
            FROM hivmigration_tmp_adverse_event_yes; 
            
            -- Adverse effect Yes checkbox
            INSERT INTO tmp_obs (
                obs_group_id, 
                value_coded_uuid, 
                source_encounter_id, 
                concept_uuid)
            SELECT
                obs_group_id,
                concept_uuid_from_mapping('CIEL', '121764'),
                source_encounter_id,
                concept_uuid_from_mapping('PIH', 'ADVERSE EFFECT')
            FROM hivmigration_tmp_adverse_event_yes;
        ''')

        migrate_tmp_obs()
    }

    @Override
    def void revert() {
        executeMysql("drop table if exists hivmigration_tmp_adverse_event")
        executeMysql("drop table if exists hivmigration_tmp_adverse_event_yes")
        clearTable("obs")
    }
}
