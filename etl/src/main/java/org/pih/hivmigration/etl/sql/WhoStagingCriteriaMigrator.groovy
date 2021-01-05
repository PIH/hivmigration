package org.pih.hivmigration.etl.sql

class WhoStagingCriteriaMigrator extends ObsMigrator {

    @Override
    def void migrate() {
        executeMysql("Create staging table for HIV_EXAM_WHO_STAGING_CRITERIA", '''
            create table hivmigration_hiv_who_staging (                                          
              source_encounter_id int,
              criterium VARCHAR(72)    
            );
        ''')

        loadFromOracleToMySql('''
            insert into hivmigration_hiv_who_staging (
              source_encounter_id,
              criterium
            )
            values(?,?) 
            ''', '''
            SELECT 
                w.ENCOUNTER_ID as source_encounter_id, 
                w.CRITERIUM 
            from HIV_EXAM_WHO_STAGING_CRITERIA w, hiv_encounters e, hiv_demographics_real d 
            where w.CRITERIUM is not null and w.ENCOUNTER_ID = e.ENCOUNTER_ID and e.patient_id = d.patient_id;
            ''')

        executeMysql("Create table for mapping WHO staging symptoms to OpenMRS Concepts", '''
            create table hivmigration_hiv_who_stage_mapping (                            
              criterium VARCHAR(72) PRIMARY KEY,
              openmrs_concept_source VARCHAR(32),
              openmrs_concept_code VARCHAR(64)           
            );
        ''')

        executeMysql("Add HIV WHO stage mappings", '''
            insert into hivmigration_hiv_who_stage_mapping(
                criterium,
                openmrs_concept_source,
                openmrs_concept_code) 
            values
                ('acute_necrotizing_ulcerative_stomatitis_gingivitis_periodontitis', 'CIEL', '149629'),
                ('angular_cheilitis', 'CIEL', '148762'),
                ('asymptomatic', 'CIEL', '5327'),
                ('atypical_disseminated_leishmaniasis', 'CIEL', '159338'),                
                ('candidiasis_of_esophagus_trachea_bronchi_lungs', 'CIEL', '5340'),
                ('chronic_cryptosporidiosis_over_1_month', 'CIEL', '5034'),
                ('chronic_herpes_simplex', 'CIEL', '138706'),
                ('cns_toxoplasmosis', 'CIEL', '990'),
                ('chronic_isosporiasis', 'CIEL', '160520'),
                ('cytomegalovirus_infection', 'CIEL', '5035'),
                ('disseminated_mycosis', 'CIEL', '5350'),                                  
                ('disseminated_non_tb_mycobacteria', 'CIEL', '5043'),
                ('extrapulmonary_tb', 'CIEL', '5042'),
                ('fungal_nail_infection', 'CIEL', '132387'),
                ('herpes_zoster', 'CIEL', '117543'),
                ('hiv_encephalopathy', 'CIEL', '160442'),
                ('hiv_wasting', 'CIEL', '823'),
                ('invasive_cervical_carcinoma', 'CIEL', '116023'),
                ('kaposis_sarcoma', 'CIEL', '507'),
                ('lymphoma_cerebra_or_b_cell_non_hodgkin', 'CIEL', '115195'),                
                ('oral_hairy_leukoplakia', 'CIEL', '5337'),
                ('papular_pruritic_eruptions', 'CIEL', '1249'),
                ('persistent_generalized_lymphadenopathy', 'CIEL', '5328'),
                ('persistent_oral_candidiasis', 'PIH', '2580'),
                ('pneumocystis_pneumonia', 'CIEL', '130021'),
                ('progressive_multifocal_leukoencephalopathy', 'CIEL', '5046'),                
                ('pulmonary_tuberculosis_current', 'CIEL', '42'),
                ('recurrent_oral_ulceration', 'CIEL', '159912'),
                ('recurrent_severe_bacterial_pneumonia', 'CIEL', '1215'),
                ('recurrent_upper_respitory_infections', 'CIEL', '127784'),
                ('seborrhoeic_dermatitis', 'CIEL', '113116'),
                ('symptomatic_nephropathy_or_cardiomyopathy_associated_with_hiv', 'PIH', '2721'),                
                ('recurrent_septicaemia', 'PIH', '11407'),                
                ('severe_bacterial_infections', 'CIEL', '5030'),
                ('unexplained_anaemia_neutropaenia_thrombocytopaenia_over_1_month', 'PIH', '2582'),
                ('unexplained_chronic_diarrhea_over_1_month', 'CIEL', '5018'),
                ('unexplained_persistent_fevre', 'CIEL', '5027'),
                ('unexplained_weight_loss_over_10_percent', 'CIEL', '5339'),
                ('unexplained_weight_loss_under_10_percent', 'CIEL', '5332')            
            ''')

        create_tmp_obs_table()

        executeMysql("Load WHO Clinical Staging observations", '''

            INSERT INTO tmp_obs (
                source_encounter_id,
                concept_uuid,
                value_coded_uuid)
            select 
                s.source_encounter_id,  
                concept_uuid_from_mapping('CIEL', '6042') as concept_uuid,
                concept_uuid_from_mapping(m.openmrs_concept_source, m.openmrs_concept_code) as value_coded_uuid
            from hivmigration_hiv_who_staging s, hivmigration_hiv_who_stage_mapping m 
            where s.criterium = m.criterium;
        ''')

        migrate_tmp_obs()
    }

    @Override
    def void revert() {
        executeMysql("drop table if exists hivmigration_hiv_who_stage_mapping")
        executeMysql("drop table if exists hivmigration_hiv_who_staging")
    }
}
