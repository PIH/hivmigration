package org.pih.hivmigration.etl.sql

class AdherenceMigrator extends ObsMigrator {

    @Override
    def void migrate() {
        // the hivmigration_observations table gets created by ObsLoadingMigrator

        create_tmp_obs_table()

        executeMysql("Migrate adherence data into tmp_obs table", '''
            INSERT INTO tmp_obs
                (source_encounter_id, obs_datetime, concept_uuid, value_numeric)
            SELECT source_encounter_id,
                   entry_date,
                   concept_uuid_from_mapping('CIEL', '160110'),
                   trim(value)
            FROM hivmigration_observations
            WHERE observation = 'num_doses_missed_last_month' AND is_number(trim(value));
            
            INSERT INTO tmp_obs
                (source_encounter_id, obs_datetime, concept_uuid, value_coded_uuid)
            SELECT source_encounter_id,
                   entry_date,
                   concept_uuid_from_mapping('CIEL', '160111'),
                   IF( value = 't',
                       concept_uuid_from_mapping('CIEL', '1065'),
                       concept_uuid_from_mapping('CIEL', '1066'))
            FROM hivmigration_observations
            WHERE observation =  'visted_by_accomp_daily_p' AND value IS NOT NULL;
            
            INSERT INTO tmp_obs
                (source_encounter_id, obs_datetime, concept_uuid, value_text)
            SELECT source_encounter_id,
                   entry_date,
                   concept_uuid_from_mapping('PIH', '13065'),
                   value
            FROM hivmigration_observations
            WHERE observation =  'accompagnateur_missed_visits_reason';
        ''')

        migrate_tmp_obs()

        executeMysql("Warn about invalid data", '''
            INSERT INTO hivmigration_data_warnings
                (hiv_emr_encounter_id, field_name, field_value, warning_type)
            SELECT source_encounter_id, 'num_doses_missed_last_month', value, 'Not a valid number. Value not migrated.'
            FROM hivmigration_observations
            WHERE observation = 'num_doses_missed_last_month' AND NOT is_number(trim(value));
        ''')
    }

    @Override
    def void revert() {
        executeMysql('''
            DELETE FROM obs WHERE concept_id IN (
                concept_from_mapping('CIEL', '160110'),
                concept_from_mapping('CIEL', '160111'),
                concept_from_mapping('PIH', '13065')
            );
        ''')
    }
}
