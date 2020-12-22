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
                ('genito_urinary_syphilis', '',''),
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
                ('pneumopathy', '',''),
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
                ('tuberculoma', '', ''),
                ('tuberculosis', 'CIEL','112141'),
                ('typhoid', 'CIEL','141'),
                ('vaginal_urethral_discharge', 'CIEL','123529'),
                ('vaginal_urethral_gc', '',''),
                ('vaginal_urethral_other', '',''),
                ('warts', '',''),
                ('wasting', 'CIEL','823'),
                ('weight_loss', 'CIEL','832')                     
            ''')

        executeMysql("Create staging table for HIV_EXAMS", '''
            create table hivmigration_hiv_exam_ois (                            
              obs_id int PRIMARY KEY AUTO_INCREMENT,
              source_encounter_id int,
              source_patient_id int,
              oi VARCHAR(32),
              comments VARCHAR(128)     
            );
        ''')

        setAutoIncrement("hivmigration_hiv_exam_ois", "(select max(obs_id)+1 from obs)")

        loadFromOracleToMySql('''
            insert into hivmigration_hiv_exam_ois (
              source_encounter_id,
              source_patient_id,
              oi,              
              comments 
            )
            values(?,?,?,?) 
            ''', '''
            select 
                x.ENCOUNTER_ID as source_encounter_id, 
                e.patient_id as source_patient_id, 
                lower(trim(x.oi)) as oi,                  
                x.COMMENTS
            from HIV_EXAM_OIS x, HIV_ENCOUNTERS e, hiv_demographics_real d  
            where x.ENCOUNTER_ID = e.ENCOUNTER_ID and e.patient_id = d.patient_id;
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
            where x.oi = 'none';
            
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
            where x.oi is not null and x.oi != 'none'; 
            
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
            where x.oi is not null and x.oi != 'none';  
            
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
            where x.oi is not null and x.oi != 'none'; 
            
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
            where x.oi is not null and x.oi != 'none' and x.oi != 'other' and x.oi = m.oi 
                and concept_uuid_from_mapping(m.openmrs_concept_source, m.openmrs_concept_code) is not null;  
            
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
            where x.oi = 'other'; 
            
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
            where x.oi is not null and x.oi != 'none' and x.oi != 'other' and x.oi = m.oi 
                and concept_uuid_from_mapping(m.openmrs_concept_source, m.openmrs_concept_code) is null; 
                                             
        ''')

        migrate_tmp_obs()
    }

    @Override
    def void revert() {
        executeMysql("drop table if exists hivmigration_hiv_ois_mapping")
        executeMysql("drop table if exists hivmigration_hiv_exam_ois")
    }
}
