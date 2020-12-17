package org.pih.hivmigration.etl.sql

class DiagnosisMigrator extends ObsMigrator {

    @Override
    def void migrate() {
        // hivmigration_diagnoses and hivmigration_patient_diagnoses tables are created by StagingTablesMigrator

        executeMysql("Create temporary table for patient diagnosis groups", '''
            CREATE TABLE hivmigration_tmp_diagnosis_groups (
                obs_group_id INT PRIMARY KEY AUTO_INCREMENT,
                source_encounter_id INT,
                source_patient_id INT,
                diagnosis_concept_uuid VARCHAR(38),
                start_date DATE,
                comments VARCHAR(256),
                present BOOL
            );
        ''')

        setAutoIncrement("hivmigration_tmp_diagnosis_groups", "SELECT max(obs_id)+1 from obs")

        executeMysql("Create obs groups for patient diagnosis history", '''
            INSERT INTO hivmigration_tmp_diagnosis_groups
            (diagnosis_concept_uuid, source_encounter_id, source_patient_id, comments, present, start_date)
            SELECT
                CASE dx.diagnosis_eng
                    WHEN 'Anemia' THEN concept_uuid_from_mapping('PIH', 'ANEMIA')
                    WHEN 'Dermatitis' THEN concept_uuid_from_mapping('PIH', 'DERMATITIS')
                    WHEN 'Diabetes' THEN concept_uuid_from_mapping('PIH', 'DIABETES')
                    WHEN 'Diarrhea' THEN concept_uuid_from_mapping('PIH', 'DIARRHEA')
                    WHEN 'Enteropathy' THEN concept_uuid_from_mapping('CIEL', '119175')
                    WHEN 'Hepatitis' THEN concept_uuid_from_mapping('PIH', 'HEPATITIS')
                    WHEN 'Hypertension' THEN concept_uuid_from_mapping('PIH', 'HYPERTENSION')
                    WHEN 'Malaria diagnosed in past 12 months' THEN concept_uuid_from_mapping('PIH', 'MALARIA IN THE LAST TWELVE MONTHS')
                    WHEN 'Nephropathy' THEN concept_uuid_from_mapping('CIEL', '153701')
                    WHEN 'Neuropathy' THEN concept_uuid_from_mapping('CIEL', '118983')
                    WHEN 'Other cardiac problems' THEN concept_uuid_from_mapping('CIEL', '119270')
                    WHEN 'Other neurologic symptoms' THEN concept_uuid_from_mapping('PIH', '995')
                    WHEN 'Pneumonia' THEN concept_uuid_from_mapping('PIH', 'PNEUMONIA')
                    WHEN 'Seizure / convulsions' THEN concept_uuid_from_mapping('PIH', 'SEIZURE')
                    WHEN 'Sexually transmitted infection' THEN concept_uuid_from_mapping('PIH', 'SEXUALLY TRANSMITTED INFECTION')
                    WHEN 'Shingles / zoster' THEN concept_uuid_from_mapping('PIH', 'HERPES ZOSTER')
                    WHEN 'Thrush' THEN concept_uuid_from_mapping('CIEL', '5334')
                    WHEN 'Tuberculosis' THEN concept_uuid_from_mapping('PIH', 'TUBERCULOSIS')
                    END,
                e.source_encounter_id,
                e.source_patient_id,
                pt_dx.diagnosis_comments,
                pt_dx.present_p = 't',
                pt_dx.diagnosis_date
            FROM hivmigration_patient_diagnoses pt_dx
            JOIN hivmigration_diagnoses dx
                ON pt_dx.diagnosis_id = dx.diagnosis_id
            JOIN hivmigration_encounters e
                ON pt_dx.patient_id = e.source_patient_id
                    AND e.source_encounter_type = 'intake'
            GROUP BY e.source_encounter_id, dx.diagnosis_eng;  -- some results are duplicated or triplicated in the source data
            
            -- 'other' diagnoses
            INSERT INTO hivmigration_tmp_diagnosis_groups
                (diagnosis_concept_uuid, source_encounter_id, source_patient_id, comments)
            SELECT
                concept_uuid_from_mapping('PIH', 'OTHER'),
                e.source_encounter_id,
                e.source_patient_id,
                pt_dx.diagnosis_other
            FROM hivmigration_patient_diagnoses pt_dx
            JOIN hivmigration_encounters e
                 ON pt_dx.patient_id = e.source_patient_id
                 AND e.source_encounter_type = 'intake'
            WHERE diagnosis_other IS NOT NULL
            GROUP BY e.encounter_id, pt_dx.diagnosis_other;
        ''')

        create_tmp_obs_table()

        executeMysql("Populate tmp_obs table with diagnosis groups", '''
            INSERT INTO tmp_obs (obs_id, source_patient_id, source_encounter_id, concept_uuid)
            SELECT obs_group_id,
                   source_patient_id,
                   source_encounter_id,
                   concept_uuid_from_mapping('CIEL', '1633')
            FROM hivmigration_tmp_diagnosis_groups;
            
            INSERT INTO tmp_obs (obs_group_id, source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT obs_group_id,
                   source_patient_id,
                   source_encounter_id,
                   concept_uuid_from_mapping('CIEL', '1729'),
                   IF(present,
                      concept_uuid_from_mapping('PIH', 'YES'),
                      concept_uuid_from_mapping('PIH', 'NO')
                   )
            FROM hivmigration_tmp_diagnosis_groups;
            
            INSERT INTO tmp_obs (obs_group_id, source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT obs_group_id,
                   source_patient_id,
                   source_encounter_id,
                   concept_uuid_from_mapping('CIEL', '1628'),
                   diagnosis_concept_uuid
            FROM hivmigration_tmp_diagnosis_groups
            WHERE diagnosis_concept_uuid IS NOT NULL;
            
            INSERT INTO tmp_obs (obs_group_id, source_patient_id, source_encounter_id, concept_uuid, value_text)
            SELECT obs_group_id,
                   source_patient_id,
                   source_encounter_id,
                   concept_uuid_from_mapping('CIEL', '160221'),
                   comments
            FROM hivmigration_tmp_diagnosis_groups
            WHERE comments IS NOT NULL;

            INSERT INTO tmp_obs (obs_group_id, source_patient_id, source_encounter_id, concept_uuid, value_datetime)
            SELECT obs_group_id,
                   source_patient_id,
                   source_encounter_id,
                   concept_uuid_from_mapping('CIEL', '159948'),
                   start_date
            FROM hivmigration_tmp_diagnosis_groups
            WHERE start_date IS NOT NULL;
        ''')

        migrate_tmp_obs()
    }

    @Override
    def void revert() {
        executeMysql("DROP TABLE IF EXISTS hivmigration_tmp_diagnosis_groups;")
        executeMysql('''
            DELETE obs FROM obs
            JOIN obs grouping_obs ON obs.obs_group_id = grouping_obs.obs_id
            WHERE grouping_obs.concept_id = concept_from_mapping('CIEL', '1633');
        ''')
        executeMysql('''
            DELETE FROM obs WHERE concept_id = concept_from_mapping('CIEL', '1633');
        ''')
    }
}
