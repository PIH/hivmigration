package org.pih.hivmigration.etl.sql

class FormsMigrator extends ObsMigrator {
    @Override
    def void migrate() {
        // hivmigration_intake_forms gets created by StagingTablesMigrator

        create_tmp_obs_table()

        executeMysql("Migrate recommendation", '''
            INSERT INTO tmp_obs (value_text, source_encounter_id, concept_uuid)
            SELECT recommendation,
                   source_encounter_id,
                   concept_uuid_from_mapping('CIEL', '162749')
            FROM hivmigration_intake_forms WHERE recommendation IS NOT NULL;
        ''')

        executeMysql("Migrate progress", '''
            INSERT INTO tmp_obs (value_coded_uuid, source_encounter_id, concept_uuid)
            SELECT CASE progress
                        WHEN 'acceptable' THEN concept_uuid_from_mapping('PIH', 'SATISFACTORY')
                        WHEN 'deterioration' THEN concept_uuid_from_mapping('PIH', 'DETERIORATION')
                        WHEN 'excellent' THEN concept_uuid_from_mapping('PIH', 'GOOD')
                        WHEN 'good' THEN concept_uuid_from_mapping('PIH', 'GOOD')
                        WHEN 'no_progress' THEN concept_uuid_from_mapping('CIEL', '162679')
                        WHEN 'satisfactory' THEN concept_uuid_from_mapping('PIH', 'SATISFACTORY')
                        END,
                   source_encounter_id,
                   concept_uuid_from_mapping('CIEL', '160116')
            FROM hivmigration_followup_forms WHERE progress IS NOT NULL;
        ''')

        migrate_tmp_obs()
    }

    @Override
    def void revert() {
        executeMysql("DELETE FROM obs WHERE concept_id = concept_from_mapping('CIEL', '162749')")
        executeMysql("DELETE FROM obs WHERE concept_id = concept_from_mapping('CIEL', '160116')")
    }
}
