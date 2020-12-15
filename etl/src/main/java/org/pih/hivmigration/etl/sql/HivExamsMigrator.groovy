package org.pih.hivmigration.etl.sql

class HivExamsMigrator extends ObsMigrator {

    @Override
    def void migrate() {

        executeMysql("Create staging table for HIV_EXAMS", '''
            create table hivmigration_hiv_exams (                            
              obs_id int PRIMARY KEY AUTO_INCREMENT,
              source_encounter_id int,
              source_patient_id int,
              presenting_history text,
              diagnosis text,
              comments text            
            );
        ''')

        setAutoIncrement("hivmigration_hiv_exams", "(select max(obs_id)+1 from obs)")

        loadFromOracleToMySql('''
            insert into hivmigration_hiv_exams (
              source_encounter_id,
              source_patient_id,
              presenting_history,
              diagnosis,
              comments 
            )
            values(?,?,?,?,?) 
            ''', '''
            select 
                x.ENCOUNTER_ID as source_encounter_id, 
                e.patient_id as source_patient_id, 
                x.PRESENTING_HISTORY, 
                x.DIAGNOSIS, 
                x.COMMENTS
            from HIV_EXAMS x, HIV_ENCOUNTERS e, hiv_demographics_real d  
            where x.ENCOUNTER_ID = e.ENCOUNTER_ID and e.patient_id = d.patient_id;
            ''')

        create_tmp_obs_table()
        setAutoIncrement("tmp_obs", "(select max(obs_id)+1 from hivmigration_hiv_exams)")

        executeMysql("Load Presenting_History as intake History_of_present_illness observations", ''' 
            
            SET @encounter_type_intake = (select encounter_type_id from encounter_type where uuid = 'c31d306a-40c4-11e7-a919-92ebcb67fe33');
                                                                               
            INSERT INTO tmp_obs (
                source_patient_id, 
                source_encounter_id, 
                concept_uuid,                 
                value_text)
            SELECT 
                x.source_patient_id,
                x.source_encounter_id,
                concept_uuid_from_mapping('PIH', 'PRESENTING HISTORY') as concept_uuid,
                x.presenting_history
            from hivmigration_hiv_exams x, hivmigration_encounters e 
            where x.presenting_history is not null and x.source_encounter_id = e.source_encounter_id and e.encounter_type_id=@encounter_type_intake;            
        ''')

        executeMysql("Load Presenting_History as followup Chief_Complaint observations", ''' 
                        
            SET @encounter_type_followup = (select encounter_type_id from encounter_type where uuid = 'c31d3312-40c4-11e7-a919-92ebcb67fe33');
                                                                   
            INSERT INTO tmp_obs (
                source_patient_id, 
                source_encounter_id, 
                concept_uuid,                 
                value_text)
            SELECT 
                x.source_patient_id,
                x.source_encounter_id,
                concept_uuid_from_mapping('CIEL', '160531') as concept_uuid,
                x.presenting_history
            from hivmigration_hiv_exams x, hivmigration_encounters e 
            where x.presenting_history is not null and x.source_encounter_id = e.source_encounter_id and e.encounter_type_id=@encounter_type_followup;            
        ''')

        executeMysql("Load HIV_EXAMS.DIAGNOSIS as Visit Diagnosis obs construct", ''' 
            
            -- Create Visit Diagnosis obs_group                                                                                           
            INSERT INTO tmp_obs (
                obs_id,
                source_patient_id, 
                source_encounter_id, 
                concept_uuid)
            SELECT 
                x.obs_id,
                x.source_patient_id,
                x.source_encounter_id,
                concept_uuid_from_mapping('PIH', 'Visit Diagnoses') as concept_uuid
            from hivmigration_hiv_exams x 
            where x.diagnosis is not null; 
            
            -- Mark it as PRESUMED diagnosis
            INSERT INTO tmp_obs (
                obs_group_id,
                source_patient_id, 
                source_encounter_id, 
                concept_uuid,
                value_coded_uuid)
            SELECT 
                x.obs_id,
                x.source_patient_id,
                x.source_encounter_id,
                concept_uuid_from_mapping('PIH', 'CLINICAL IMPRESSION DIAGNOSIS CONFIRMED') as concept_uuid,
                concept_uuid_from_mapping('PIH', 'PRESUMED') as value_coded_uuid
            from hivmigration_hiv_exams x 
            where x.diagnosis is not null;  
            
            -- Set Diagnosis order to PRIMARY
            INSERT INTO tmp_obs (
                obs_group_id,
                source_patient_id, 
                source_encounter_id, 
                concept_uuid,
                value_coded_uuid)
            SELECT 
                x.obs_id,
                x.source_patient_id,
                x.source_encounter_id,
                concept_uuid_from_mapping('PIH', 'Diagnosis order') as concept_uuid,
                concept_uuid_from_mapping('PIH', 'primary') as value_coded_uuid
            from hivmigration_hiv_exams x 
            where x.diagnosis is not null; 
            
            -- Create Diagnosis as non-coded Diagnosis
            INSERT INTO tmp_obs (
                obs_group_id,
                source_patient_id, 
                source_encounter_id, 
                concept_uuid,
                value_text)
            SELECT 
                x.obs_id,
                x.source_patient_id,
                x.source_encounter_id,
                concept_uuid_from_mapping('PIH', 'Diagnosis or problem, non-coded') as concept_uuid,
                x.diagnosis
            from hivmigration_hiv_exams x 
            where x.diagnosis is not null;                                  
        ''')

        executeMysql("Load HIV_EXAMS.COMMENTS as Exam_Comment observations", ''' 
                                                                                                       
            INSERT INTO tmp_obs (
                source_patient_id, 
                source_encounter_id, 
                concept_uuid,                 
                value_text)
            SELECT 
                x.source_patient_id,
                x.source_encounter_id,
                concept_uuid_from_mapping('PIH', 'PHYSICAL SYSTEM COMMENT') as concept_uuid,
                x.comments
            from hivmigration_hiv_exams x 
            where x.comments is not null;            
        ''')

        migrate_tmp_obs()
    }

    @Override
    def void revert() {
        executeMysql("drop table if exists hivmigration_hiv_exams")
    }
}
