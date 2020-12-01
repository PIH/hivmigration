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
                diagnosis_concept_uuid VARCHAR(38)
            );
        ''')

        setAutoIncrement("hivmigration_tmp_diagnosis_groups", "SELECT max(obs_id)+1 from obs")

        executeMysql("Migrate patient diagnosis history into intake form", '''
            INSERT INTO hivmigration_tmp_diagnosis_groups
                (diagnosis_concept_uuid, source_encounter_id, source_patient_id)
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
                e.source_patient_id
            FROM hivmigration_patient_diagnoses pt_dx
            JOIN hivmigration_diagnoses dx
                 ON pt_dx.diagnosis_id = dx.diagnosis_id
            JOIN hivmigration_encounters e
                 ON pt_dx.patient_id = e.source_patient_id
                 AND e.source_encounter_type = 'intake\'
            WHERE present_p = 't';
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
                   concept_uuid_from_mapping('PIH', 'YES')
            FROM hivmigration_tmp_diagnosis_groups;
            
            INSERT INTO tmp_obs (obs_group_id, source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT obs_group_id,
                   source_patient_id,
                   source_encounter_id,
                   concept_uuid_from_mapping('CIEL', '1628'),
                   diagnosis_concept_uuid
            FROM hivmigration_tmp_diagnosis_groups;
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
