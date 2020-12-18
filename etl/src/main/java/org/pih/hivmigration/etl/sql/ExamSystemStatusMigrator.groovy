package org.pih.hivmigration.etl.sql

class ExamSystemStatusMigrator extends ObsMigrator{

    @Override
    def void migrate() {

        executeMysql("Create table for mapping HIV Exam System Status to OpenMRS Concepts", '''
            create table hivmigration_exam_status_mapping (                            
              system_condition VARCHAR(16) PRIMARY KEY,
              openmrs_concept_source VARCHAR(32),
              openmrs_concept_code VARCHAR(64)           
            );
        ''')

        executeMysql("Add HIV Exam System Status mappings", '''
            insert into hivmigration_exam_status_mapping(
                system_condition,
                openmrs_concept_source,
                openmrs_concept_code) 
            values
                ('abdominal', 'PIH', 'ABDOMINAL EXAM FINDINGS'),
                ('cardiovascular', 'PIH', 'CARDIAC EXAM FINDINGS'),
                ('conjunctiva', 'PIH', 'HEENT EXAM FINDINGS'),
                ('extremities', 'PIH', 'MUSCULOSKELETAL EXAM FINDINGS'),
                ('general', 'PIH', 'GENERAL EXAM FINDINGS'),
                ('lymph_nodes', 'CIEL', '1121'),
                ('neurologic', 'PIH', 'NEUROLOGIC EXAM FINDINGS'),
                ('oropharynx', 'PIH', 'HEENT EXAM FINDINGS'),
                ('pulmonary', 'PIH', 'CHEST EXAM FINDINGS'),
                ('scleras', 'PIH', 'HEENT EXAM FINDINGS'),
                ('skin', 'PIH', 'SKIN EXAM FINDINGS'),
                ('urogenital', 'PIH', 'UROGENITAL EXAM FINDINGS'),
                ('ascites', 'PIH', 'ASCITES'),
                ('axillary', 'CIEL', '148058'),
                ('cervical', 'CIEL', '145802'),
                ('crepitance', 'PIH', 'CREPITATIONS'),
                ('discharge', 'PIH', 'GENITAL DISCHARGE'),
                ('edema', 'CIEL', '460'),
                ('hepatomegaly', 'PIH', 'HEPATOMEGALY'),
                ('icteric', 'PIH', 'ICTERIC SCLERA'),
                ('inginual', 'CIEL', '137155'),
                ('none', 'PIH', 'NORMAL'),
                ('normal', 'PIH', 'NORMAL'),
                ('pale', 'PIH', 'PALE CONJUNCTIVA'),
                ('rales', 'PIH', '6894'),
                ('splenomegaly', 'PIH', 'SPLENOMEGALY'),
                ('ulcers', 'CIEL', '864'),
                ('wheeze', 'PIH', 'WHEEZE')         
            ''')

        executeMysql("Create staging table for HIV_EXAM_SYSTEM_STATUS", '''
            create table hivmigration_exam_system_status(                                                     
              source_encounter_id int,
              source_patient_id int,
              exam VARCHAR(16),
              finding VARCHAR(16)
            );
        ''')

        loadFromOracleToMySql('''
            insert into hivmigration_exam_system_status (
              source_encounter_id,
              source_patient_id,
              exam,
              finding
            )
            values(?,?,?,?) 
            ''', '''
            SELECT 
                s.ENCOUNTER_ID as source_encounter_id, 
                e.patient_id as source_patient_id, 
                lower(TRIM(s.system)) as exam,
                lower(TRIM(s.condition)) as finding
            from HIV_EXAM_SYSTEM_STATUS s, hiv_encounters e, hiv_demographics_real d 
            where s.ENCOUNTER_ID = e.ENCOUNTER_ID and e.patient_id = d.patient_id;
            ''')

        create_tmp_obs_table()

        executeMysql("Load HIV EXAM STATUS observations", '''

            INSERT INTO tmp_obs (
                source_patient_id,
                source_encounter_id,
                concept_uuid,
                value_coded_uuid)
            SELECT
                s.source_patient_id,
                s.source_encounter_id,                
                concept_uuid_from_mapping( m.openmrs_concept_source, m.openmrs_concept_code) as concept_uuid,
                concept_uuid_from_mapping( f.openmrs_concept_source, f.openmrs_concept_code) as value_coded_uuid
            from hivmigration_exam_system_status s join hivmigration_exam_status_mapping m on s.exam = m.system_condition 
                join hivmigration_exam_status_mapping f on s.finding = f.system_condition;
        ''')

        migrate_tmp_obs()
    }

    @Override
    def void revert() {
        executeMysql("drop table if exists hivmigration_exam_status_mapping")
        executeMysql("drop table if exists hivmigration_exam_system_status")
    }
}
