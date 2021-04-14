package org.pih.hivmigration.etl.sql

class HivExamOisMigrator extends ObsMigrator {

    @Override
    def void migrate() {

        executeMysql("Create table for mapping HIV Opportunistic infection diagnoses to OpenMRS Concepts", '''
            create table hivmigration_hiv_ois_mapping (                            
              oi VARCHAR(32) PRIMARY KEY,
              openmrs_concept_source VARCHAR(32),
              openmrs_concept_code VARCHAR(64)           
            );
        ''')

        executeMysql("Add HIV Exam OIS mappings", '''
            insert into hivmigration_hiv_ois_mapping(
                oi,
                openmrs_concept_source,
                openmrs_concept_code) 
            values
                ('acute_diarrhea', 'CIEL', '1467'),
                ('anxiety', 'CIEL', '121543'),
                ('bacterial_diarrhea', 'CIEL','148023'),
                ('bacterial_meningitis', 'CIEL','121255'),
                ('bacterial_pneumonia', 'CIEL','121252'),
                ('candidiaris_esophagitis', 'CIEL','146513'),
                ('candidiaris_oropharyngeal', 'CIEL','5334'),
                ('candidiaris_other', 'CIEL','120939'),
                ('candidiaris_vaginal', 'CIEL','298'),
                ('chancroid', 'PIH','2738'),
                ('chronic_diarrhea', 'CIEL','145443'),
                ('cryptococcus_meningitis', 'CIEL','1294'),
                ('depression', 'CIEL','119537'),
                ('dysentery', 'CIEL','152'),
                ('dysphagia', 'CIEL','145443'),
                ('encephalopathy', 'CIEL','119288'),
                ('extrapulmonary_tb', 'CIEL','5042'),
                ('genital_ulcerations', 'CIEL','864'),
                ('genito_urinary_syphilis', 'CIEL','129119'),
                ('herpes_zoster', 'CIEL','117543'),
                ('hiv', 'CIEL', '1169'),
                ('hsv', 'CIEL','138706'),
                ('kaposis_sarcoma', 'CIEL','507'),
                ('lgv', 'CIEL','135462'),
                ('lymphoma', 'CIEL', '116104'),
                ('malaria', 'CIEL', '116128'),
                ('meningitis', 'CIEL', '115835'),
                ('neurologic_syphilis', 'CIEL','115257'),
                ('none', 'CIEL', '1007'),
                ('other', '', ''),
                ('other_diarrhea', 'CIEL','142412'),
                ('other_encephalopathy', 'CIEL', '119288'),
                ('other_meningitis', 'CIEL', '115835'),
                ('other_neuro', 'PIH', '995'),
                ('other_pneumonia', 'CIEL', '114100'),
                ('other_rash', 'CIEL', '512'),
                ('parasitic_diarrhea', 'PIH','DIARRHEA, PARASITE'),
                ('pcp_pneumonia', 'CIEL', '882'),
                ('pid', 'CIEL','902'),
                ('pneumopathy', 'CIEL','119137'),
                ('prurigo_nodulairis', 'PIH','1370'),
                ('pruritis', 'PIH', 'PRURITIS'),
                ('psychosis', 'CIEL','113517'),
                ('pulmonary_tb', 'CIEL','42'),
                ('seizures', 'CIEL','113054'),
                ('shingles', 'CIEL','117543'),
                ('std', 'PIH', '174'),
                ('tb_enteritis', 'CIEL','118606'),
                ('tb_meningitis', 'CIEL','111967'),
                ('thrush', 'CIEL','5334'),
                ('toxoplasmosis', 'CIEL','5355'),
                ('trichomoniasis', 'CIEL','117146'),
                ('tuberculoma', 'CIEL', '124069'),
                ('tuberculosis', 'CIEL','112141'),
                ('typhoid', 'CIEL','141'),
                ('vaginal_urethral_discharge', 'CIEL','123529'),
                ('vaginal_urethral_gc', 'CIEL', '117767'),
                ('vaginal_urethral_other', '',''),
                ('warts', 'CIEL', '123228'),
                ('wasting', 'CIEL','823'),
                ('weight_loss', 'CIEL','832')                     
            ''')

        executeMysql("Create staging table for HIV_EXAMS", '''
            create table hivmigration_hiv_exam_ois (                            
              obs_id int PRIMARY KEY AUTO_INCREMENT,
              source_encounter_id int,
              source_patient_id int,
              oi VARCHAR(32),
              comments VARCHAR(128),
              form_name VARCHAR(16),
              form_version char(1),
              migrated BOOLEAN DEFAULT FALSE     
            );
        ''')

        setAutoIncrement("hivmigration_hiv_exam_ois", "(select max(obs_id)+1 from obs)")

        loadFromOracleToMySql('''
            insert into hivmigration_hiv_exam_ois (
              source_encounter_id,
              source_patient_id,
              oi,              
              comments,
              form_name,
              form_version
            )
            values(?,?,?,?,?,?) 
            ''', '''
            select 
                x.ENCOUNTER_ID as source_encounter_id, 
                e.patient_id as source_patient_id, 
                lower(trim(x.oi)) as oi,                  
                x.COMMENTS,
                case
                    when (k.form_version is not null) then 'hiv_intake'                     
                    when (f.form_version is not null) then 'hiv_followup'                     
                    else null 
                end as form_name,
                case
                    when (k.form_version is not null) then k.form_version                    
                    when (f.form_version is not null) then f.form_version 
                    else null 
                end as form_version
            from HIV_EXAM_OIS x join HIV_ENCOUNTERS e on x.ENCOUNTER_ID = e.ENCOUNTER_ID 
                join hiv_demographics_real d on e.patient_id = d.patient_id 
                left outer join HIV_INTAKE_FORMS k on x.ENCOUNTER_ID=k.ENCOUNTER_ID 
                left outer join HIV_FOLLOWUP_FORMS f on x.ENCOUNTER_ID=f.ENCOUNTER_ID; 
            ''')

        create_tmp_obs_table()
        setAutoIncrement("tmp_obs", "(select max(obs_id)+1 from hivmigration_hiv_exam_ois)")


        executeMysql("Create No opportunistic infections checkboxes", ''' 
            
            INSERT INTO tmp_obs (                
                source_patient_id, 
                source_encounter_id, 
                concept_uuid,
                value_coded_uuid)
            SELECT                 
                x.source_patient_id,
                x.source_encounter_id,
                concept_uuid_from_mapping('PIH', '1401') as concept_uuid,                
                concept_uuid_from_mapping('CIEL', '1107') as value_coded_uuid
            from hivmigration_hiv_exam_ois x 
            where x.oi = 'none' and x.form_version='3';
            
            ''')

        executeMysql("Load HIV_EXAM_OIS diagnosis as Visit Diagnosis obs construct", ''' 
            
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
            from hivmigration_hiv_exam_ois x 
            where x.oi is not null and x.oi != 'none' and x.form_version='3'; 
            
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
            from hivmigration_hiv_exam_ois x 
            where x.oi is not null and x.oi != 'none' and x.form_version='3';  
            
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
            from hivmigration_hiv_exam_ois x 
            where x.oi is not null and x.oi != 'none' and x.form_version='3'; 
            
            -- Create Coded Diagnosis            
            INSERT INTO tmp_obs (
                obs_group_id,
                source_patient_id, 
                source_encounter_id, 
                concept_uuid,
                value_coded_uuid,
                comments)
            SELECT 
                x.obs_id,
                x.source_patient_id,
                x.source_encounter_id,
                concept_uuid_from_mapping('PIH', 'DIAGNOSIS') as concept_uuid,
                concept_uuid_from_mapping(m.openmrs_concept_source, m.openmrs_concept_code) as value_coded_uuid,
                x.comments
            from hivmigration_hiv_exam_ois x, hivmigration_hiv_ois_mapping m  
            where x.oi is not null and x.form_version='3' and x.oi != 'none' and x.oi != 'other' and x.oi != 'candidiaris_other' and x.oi = m.oi 
                and m.openmrs_concept_source != '' and m.openmrs_concept_code != '';  
            
            -- Create non-coded Diagnosis for other diagnoses
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
                x.comments
            from hivmigration_hiv_exam_ois x 
            where x.oi = 'other' and x.comments is not null and x.form_version='3'; 
            
             -- Create non-coded Diagnosis for candidiaris_other
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
                CONCAT('Candidiasis (' , IFNULL(x.comments, ''), ')') as value_text
            from hivmigration_hiv_exam_ois x 
            where x.oi = 'candidiaris_other' and x.form_version='3'; 
            
            -- Create non-coded Diagnoses for OI that do not have a mapping yet            
            INSERT INTO tmp_obs (
                obs_group_id,
                source_patient_id, 
                source_encounter_id, 
                concept_uuid,
                value_text,
                comments)
            SELECT 
                x.obs_id,
                x.source_patient_id,
                x.source_encounter_id,
                concept_uuid_from_mapping('PIH', 'Diagnosis or problem, non-coded') as concept_uuid,
                x.oi,
                x.comments
            from hivmigration_hiv_exam_ois x, hivmigration_hiv_ois_mapping m  
            where x.oi is not null and x.form_version='3' and x.oi != 'none' and x.oi != 'other' and x.oi != 'candidiaris_other' and x.oi = m.oi 
                and (m.openmrs_concept_source = '' or m.openmrs_concept_code = ''); 
                                             
            -- Set migrated=TRUE for all obs with form_version=3
            UPDATE hivmigration_hiv_exam_ois SET migrated=TRUE where oi is not null and form_version='3';    
            
            -- Create PIH:CLINICAL IMPRESSION COMMENTS obs for encounter's comments(UHM-5296)
            INSERT INTO tmp_obs (                                 
                source_encounter_id, 
                concept_uuid,
                value_text)
            SELECT                 
                e.source_encounter_id,
                concept_uuid_from_mapping('PIH', 'CLINICAL IMPRESSION COMMENTS') as concept_uuid,
                e.comments
            from hivmigration_encounters e 
            where e.comments is not null and e.source_encounter_type in ('intake', 'followup');                     
        ''')

        migrate_tmp_obs()
    }

    @Override
    def void revert() {
        executeMysql("drop table if exists hivmigration_hiv_ois_mapping")
        executeMysql("drop table if exists hivmigration_hiv_exam_ois")
    }
}
