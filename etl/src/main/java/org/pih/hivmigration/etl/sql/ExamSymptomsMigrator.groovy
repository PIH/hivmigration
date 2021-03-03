package org.pih.hivmigration.etl.sql

class ExamSymptomsMigrator extends ObsMigrator{

    @Override
    void migrate() {
        
        executeMysql("Create table for mapping HIV_EXAM_SYMPTOMS to OpenMRS Symptoms", '''
            create table hivmigration_symptoms_map (                            
              hiv_symptom VARCHAR(24) PRIMARY KEY,
              openmrs_symptom_name VARCHAR(24),              
              concept_source_map VARCHAR(7),
              openmrs_concept_code VARCHAR(15)              
            );
        ''')

        executeMysql("Add HIV symptoms mapping", '''
            insert into hivmigration_symptoms_map(
                hiv_symptom,
                openmrs_symptom_name,                
                concept_source_map, 
                openmrs_concept_code) 
            values                
                ('confusion', 'Confusion', 'CIEL', '120345'),                
                ('convulsions', 'Seizure', 'CIEL', '113054'),   
                ('diarrhea', 'Diarrhea', 'CIEL', '142412'),
                ('dysphagia', 'Dysphagia', 'CIEL', '118789'),
                ('genital_discharge', 'GenitalDischarge', 'PIH', '1816'),
                ('genital_ulcers', 'UlcérationsGénitales', 'CIEL', '864'),
                ('headache', 'Headache', 'CIEL', '139084'),
                ('icterus', 'Jaundice', 'CIEL', '136443'),   
                ('nausea', 'Nausea', 'CIEL', '5978'),
                ('neurologic_deficit', 'DéficitNeurologiqueFocal', 'CIEL', '1466'),
                ('paresthesia', 'Paresthesia', 'CIEL', '6004'),
                ('prurigo_nodularis', 'PrurigoNodularis', 'CIEL', '128321'),
                ('rash', 'Rash', 'CIEL', '512'),
                ('vision_problems', 'VisionProblem', 'CIEL', '118938'),
                ('vomiting', 'Vomiting', 'CIEL', '122983')                                                                 
            ''')

        executeMysql("Create staging table for migrating HIV_EXAM_SYMPTOMS", '''
            create table hivmigration_exam_symptoms (                            
              obs_id int PRIMARY KEY AUTO_INCREMENT, 
              source_encounter_id int,
              symptom VARCHAR(255),
              symptom_present BOOLEAN,
              symptom_date DATE,
              duration int,
              duration_unit VARCHAR(8),
              symptom_comment VARCHAR(264)                                             
            );
        ''')

        setAutoIncrement("hivmigration_exam_symptoms", "(select max(obs_id)+1 from obs)")

        loadFromOracleToMySql('''
            insert into hivmigration_exam_symptoms (
              source_encounter_id,
              symptom,
              symptom_present,
              symptom_date,
              duration,
              duration_unit,
              symptom_comment
            )
            values(?,?,?,?,?,?,?) 
            ''', '''
            select 
                s.encounter_id as source_encounter_id, 
                lower(s.symptom) as symptom, 
                case 
                    when (s.result = 't') then 1 
                    when (s.result = 'f') then 0 
                    else null 
                end as symptom_present,
                to_char(s.SYMPTOM_DATE, 'yyyy-mm-dd') as symptom_date,
                s.DURATION, 
                s.DURATION_UNIT, 
                s.SYMPTOM_COMMENT  
            from HIV_EXAM_SYMPTOMS s, HIV_ENCOUNTERS e, hiv_demographics_real d 
            where (trim(s.symptom) is not null) and 
                (s.SYMPTOM in ('confusion', 'convulsions', 'diarrhea', 'dysphagia', 'genital_discharge', 'genital_ulcers', 'headache', 'icterus', 'nausea', 'neurologic_deficit', 'paresthesia', 'prurigo_nodularis', 'rash', 'vision_problems', 'vomiting', 'other'))  
                and s.ENCOUNTER_ID = e.ENCOUNTER_ID and e.patient_id = d.patient_id;
        ''')

        create_tmp_obs_table()
        setAutoIncrement("tmp_obs", "(select max(obs_id)+1 from hivmigration_exam_symptoms)")

        executeMysql("Load Symptoms observations", ''' 
                      
            -- Create Signs and Symptoms obs_group
            INSERT INTO tmp_obs(
                obs_id,                 
                source_encounter_id, 
                concept_uuid) 
            SELECT 
                s.obs_id,                 
                s.source_encounter_id, 
                concept_uuid_from_mapping('CIEL', '1727') as concept_uuid
            FROM hivmigration_exam_symptoms s, hivmigration_encounters e 
            WHERE s.symptom is not null and s.source_encounter_id = e.source_encounter_id and e.form_version=3 and e.source_encounter_type='intake';

            -- Create Coded Symptom Name           
            INSERT INTO tmp_obs (
                obs_group_id,                
                source_encounter_id, 
                concept_uuid,
                value_coded_uuid,
                comments)
            SELECT 
                s.obs_id,                
                s.source_encounter_id,
                concept_uuid_from_mapping('CIEL', '1728') as concept_uuid, -- Sign/Symptom name
                concept_uuid_from_mapping(m.concept_source_map, m.openmrs_concept_code) as value_coded_uuid,
                s.symptom_comment
            from hivmigration_exam_symptoms s, hivmigration_symptoms_map m, hivmigration_encounters e  
            where s.symptom is not null and s.symptom = m.hiv_symptom 
                and m.concept_source_map != '' and m.openmrs_concept_code != '' and 
                s.source_encounter_id = e.source_encounter_id and e.form_version=3 and e.source_encounter_type='intake'; 

            -- Create Non-Coded Symptom Name           
            INSERT INTO tmp_obs (
                obs_group_id,                
                source_encounter_id, 
                concept_uuid,
                value_coded_uuid,
                comments)
            SELECT 
                s.obs_id,                
                s.source_encounter_id,
                concept_uuid_from_mapping('CIEL', '1728') as concept_uuid, -- Sign/Symptom name
                concept_uuid_from_mapping('CIEL', '5622') as value_coded_uuid, -- Other non-coded
                case
                  when (lower(s.symptom) = 'other' and s.symptom_comment is not null) then TRIM(SUBSTRING(s.symptom_comment, 0, 254))                   
                  else null 
                end as comments                  
            from hivmigration_exam_symptoms s, hivmigration_encounters e   
            where lower(s.symptom) = 'other' and s.source_encounter_id = e.source_encounter_id 
                and e.form_version=3 and e.source_encounter_type='intake';     

            -- Create Sympton present(Yes/No) obs
            INSERT INTO tmp_obs (
                obs_group_id,                
                source_encounter_id, 
                concept_uuid,
                value_coded_uuid)
            SELECT 
                s.obs_id,                
                s.source_encounter_id,
                concept_uuid_from_mapping('CIEL', '1729') as concept_uuid, -- Sign/Symptom present
                case 
                    when (s.symptom_present = true) then concept_uuid_from_mapping('CIEL', '1065')  
                    when (s.symptom_present = false) then concept_uuid_from_mapping('CIEL', '1066')                       
                    else null 
                end as value_coded_uuid
            from hivmigration_exam_symptoms s, hivmigration_encounters e    
            where s.symptom is not null and s.symptom_present is not null and 
                s.source_encounter_id = e.source_encounter_id 
                and e.form_version=3 and e.source_encounter_type='intake';

            -- Create Sympton duration obs
            INSERT INTO tmp_obs (
                obs_group_id,                
                source_encounter_id, 
                concept_uuid,
                value_numeric)
            SELECT 
                s.obs_id,                
                s.source_encounter_id,
                concept_uuid_from_mapping('CIEL', '1731') as concept_uuid, -- Sign/Symptom duration
                s.duration
            from hivmigration_exam_symptoms s, hivmigration_encounters e   
            where s.symptom is not null and s.duration is not null and s.duration_unit is not null 
                and s.source_encounter_id = e.source_encounter_id 
                and e.form_version=3 and e.source_encounter_type='intake';

            -- Create Sympton duration units obs
            INSERT INTO tmp_obs (
                obs_group_id,                
                source_encounter_id, 
                concept_uuid,
                value_coded_uuid)
            SELECT 
                s.obs_id,                
                s.source_encounter_id,
                concept_uuid_from_mapping('CIEL', '1732') as concept_uuid, -- Time units
                case 
                    when (s.duration_unit = 'years') then concept_uuid_from_mapping('CIEL', '1734')  
                    when (s.duration_unit = 'months') then concept_uuid_from_mapping('CIEL', '1074') 
                    when (s.duration_unit = 'weeks') then concept_uuid_from_mapping('CIEL', '1073') 
                    when (s.duration_unit = 'days') then concept_uuid_from_mapping('CIEL', '1072')                     
                    else null 
                end as value_coded_uuid
            from hivmigration_exam_symptoms s, hivmigration_encounters e  
            where s.symptom is not null and s.duration is not null and s.duration_unit is not null 
                and s.source_encounter_id = e.source_encounter_id 
                and e.form_version=3 and e.source_encounter_type='intake'; 

        ''')

      migrate_tmp_obs()
        
    }

    @Override
    void revert() {
        executeMysql("drop table if exists hivmigration_exam_symptoms")
        executeMysql("drop table if exists hivmigration_symptoms_map")
    }
}
