package org.pih.hivmigration.etl.sql

class AdverseEventMigrator  extends ObsMigrator {

    @Override
    def void migrate() {
        executeMysql("Create staging table", '''
            create table hivmigration_adverse_event (
              obs_id int PRIMARY KEY AUTO_INCREMENT,
              source_patient_id int,
              source_encounter_id int,
              reason VARCHAR(64),
              type_of_reaction VARCHAR(16),
              allergy_date date
            );
        ''')

        setAutoIncrement("hivmigration_adverse_event", "(select max(obs_id)+1 from obs)")

        loadFromOracleToMySql('''
            insert into hivmigration_adverse_event
               (source_patient_id,
                reason,                                
                type_of_reaction,
                allergy_date)
            values (?, ?, ?, ?)
        ''', '''
            select 
                a.patient_id, 
                case when (a.INN is not null) then a.INN else a.OTHER_REASON end as reason, 
                a.TYPE_OF_REACTION, 
                to_char(a.ALLERGY_DATE, 'yyyy-mm-dd') as allergy_date  
            from HIV_ALLERGIES a, hiv_demographics_real p where a.patient_id = p.patient_id
        ''')

        executeMysql('''
            update hivmigration_adverse_event a, hivmigration_encounters e 
            set a.source_encounter_id = e.source_encounter_id 
            where a.source_patient_id = e.source_patient_id and e.source_encounter_type='intake' ;
        ''')

        create_tmp_obs_table()
        setAutoIncrement("tmp_obs", "(select max(obs_id)+1 from hivmigration_adverse_event)")

        executeMysql("Load adverse events as observations",
                '''

            -- Adverse effect construct
            INSERT INTO tmp_obs (obs_id, source_patient_id, source_encounter_id, concept_uuid)
            SELECT
                obs_id,                     
                source_patient_id,
                source_encounter_id,
                concept_uuid_from_mapping('PIH', 'ADVERSE EFFECT CONSTRUCT')
            FROM hivmigration_adverse_event; 

            -- Reason for the Adverse effect
            INSERT INTO tmp_obs (
                obs_group_id, 
                value_text, 
                source_patient_id, 
                source_encounter_id, 
                concept_uuid)
            SELECT  
                obs_id,
                reason,
                source_patient_id,
                source_encounter_id,
                concept_uuid_from_mapping('PIH', 'GENERAL FREE TEXT')
            FROM hivmigration_adverse_event 
            WHERE reason is not null; 
            
            -- Adverse effect
            INSERT INTO tmp_obs (
                obs_group_id, 
                value_coded_uuid, 
                source_patient_id, 
                source_encounter_id, 
                concept_uuid, 
                comments)
            SELECT  
                obs_id,
                CASE type_of_reaction 
                    WHEN 'Rash' THEN concept_uuid_from_mapping('CIEL', '512') 
                    WHEN 'Neuropathy' THEN concept_uuid_from_mapping('CIEL', '118983')
                    WHEN 'Aniphylaxis' THEN concept_uuid_from_mapping('CIEL', '148888')
                    ELSE concept_uuid_from_mapping('CIEL', '5622') 
                END value_coded_uuid,                        
                source_patient_id,
                source_encounter_id,
                concept_uuid_from_mapping('PIH', 'ADVERSE EFFECT') as concept_uuid,
                IF(type_of_reaction not in ('Rash', 'Neuropathy', 'Aniphylaxis'), type_of_reaction, NULL) as comments
            FROM hivmigration_adverse_event 
            WHERE type_of_reaction is not null; 
            
            -- Adverse effect date
            INSERT INTO tmp_obs (
                obs_group_id, 
                value_datetime, 
                source_patient_id, 
                source_encounter_id, 
                concept_uuid)
            SELECT  
                obs_id,
                allergy_date,
                source_patient_id,
                source_encounter_id,
                concept_uuid_from_mapping('PIH', 'ADVERSE EFFECT DATE')
            FROM hivmigration_adverse_event 
            WHERE allergy_date is not null; 
            
        ''')

        migrate_tmp_obs()
    }

    @Override
    def void revert() {
        executeMysql("drop table if exists hivmigration_adverse_event")
        clearTable("obs")
    }
}
