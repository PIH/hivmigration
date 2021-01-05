package org.pih.hivmigration.etl.sql

class SocioEconomicsMigrator extends ObsMigrator {
    @Override
    def void migrate() {
        // loaded by StagingTablesMigrator

        create_tmp_obs_table()

        executeMysql("Add source_encounter_id to staged table", '''
            ALTER TABLE hivmigration_socioeconomics_extra
            ADD COLUMN source_encounter_id INT;
            
            UPDATE hivmigration_socioeconomics_extra hse
            JOIN hivmigration_encounters he
                ON hse.patient_id = he.patient_id
                AND he.source_encounter_type = 'intake'
            SET source_encounter_id = he.source_encounter_id;
        ''')

        executeMysql("Migrate tobacco and alcohol use", '''
            INSERT INTO tmp_obs
                (source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT source_encounter_id,
                   concept_uuid_from_mapping('CIEL', '163731'),
                   IF( smokes_p = 't',
                       concept_uuid_from_mapping('PIH', 'CURRENTLY'),
                       concept_uuid_from_mapping('PIH', 'NO'))
            FROM hivmigration_socioeconomics_extra
            WHERE smokes_p IN ('t', 'f');
            
            INSERT INTO tmp_obs
                (source_encounter_id, concept_uuid, value_numeric)
            SELECT source_encounter_id,
                   concept_uuid_from_mapping('CIEL', '159931'),
                   trim(smoking_years)
            FROM hivmigration_socioeconomics_extra
            WHERE smoking_years IS NOT NULL;
            
            INSERT INTO tmp_obs
                (source_encounter_id, concept_uuid, value_numeric)
            SELECT source_encounter_id,
                   concept_uuid_from_mapping('PIH', '11949'),
                   trim(num_cigarretes_per_day)
            FROM hivmigration_socioeconomics_extra
            WHERE num_cigarretes_per_day IS NOT NULL;
            
            INSERT INTO tmp_obs
                (source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT source_encounter_id,
                   concept_uuid_from_mapping('CIEL', '159449'),
                   concept_uuid_from_mapping('PIH', 'CURRENTLY')
            FROM hivmigration_socioeconomics_extra
            WHERE num_days_alcohol_per_week > 0 OR beer_per_day > 0 OR wine_per_day > 0 OR drinks_per_day > 0;
            
            INSERT INTO tmp_obs
                (source_encounter_id, concept_uuid, value_numeric)
            SELECT source_encounter_id,
                   concept_uuid_from_mapping('PIH', '2243'),
                   trim(num_days_alcohol_per_week)
            FROM hivmigration_socioeconomics_extra
            WHERE num_days_alcohol_per_week IS NOT NULL;
            
            INSERT INTO tmp_obs
                (source_encounter_id, concept_uuid, value_numeric)
            SELECT source_encounter_id,
                   concept_uuid_from_mapping('PIH', '2246'),
                   (IFNULL(trim(wine_per_day), 0) + IFNULL(trim(beer_per_day), 0) + IFNULL(trim(drinks_per_day), 0))
            FROM hivmigration_socioeconomics_extra
            WHERE wine_per_day IS NOT NULL OR beer_per_day IS NOT NULL OR drinks_per_day IS NOT NULL;
        ''')

        migrate_tmp_obs()

    }

    @Override
    def void revert() {

    }
}
