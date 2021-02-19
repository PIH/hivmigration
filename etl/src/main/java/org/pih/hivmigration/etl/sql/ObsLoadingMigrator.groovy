package org.pih.hivmigration.etl.sql

/**
 * This simply loads hivmigration_observations from Oracle.
 */
class ObsLoadingMigrator extends SqlMigrator {
    @Override
    def void migrate() {
        executeMysql("Create staging table for observations", '''
            CREATE TABLE hivmigration_observations (
              source_observation_id int,
              source_encounter_id int,
              observation varchar(100),
              value varchar(4000),
              entry_date date,
              observation_uuid char(38),
              KEY `source_observation_idx` (`source_observation_id`),
              KEY `source_encounter_idx` (`source_encounter_id`),
              KEY `observation_idx` (`observation`),
              UNIQUE KEY `uuid_idx` (`observation_uuid`)
            );
        ''')

        loadFromOracleToMySql('''
            INSERT INTO hivmigration_observations (source_observation_id, source_encounter_id, observation, value, entry_date)
            VALUES (?, ?, ?, ?, ?)
        ''', '''
            SELECT o.observation_id, o.encounter_id, o.observation, o.value, o.entry_date
            FROM hiv_observations o, hiv_encounters e, hiv_demographics_real d 
            WHERE o.encounter_id=e.encounter_id and e.patient_id = d.patient_id 
        ''')
    }

    @Override
    def void revert() {
        executeMysql("DROP TABLE IF EXISTS hivmigration_observations")
    }
}
