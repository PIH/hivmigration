package org.pih.hivmigration.etl.sql

import org.apache.commons.dbutils.handlers.ScalarHandler
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory

abstract class ObsMigrator extends SqlMigrator {

    private static final Log log = LogFactory.getLog(ObsMigrator.class);

    void create_tmp_obs_table() {
        executeMysql("Create tmp_obs table", '''
            DROP TABLE IF EXISTS tmp_obs;
            CREATE TABLE tmp_obs (
                obs_id INT PRIMARY KEY AUTO_INCREMENT,
                obs_group_id INT,
                source_patient_id INT,  -- optional, defaults to the one from source_encounter_id
                source_encounter_id INT,
                concept_uuid CHAR(38),
                value_coded_uuid CHAR(38),
                value_drug_uuid CHAR(38),
                value_datetime DATETIME,
                value_numeric DOUBLE,
                value_text TEXT
            );
        ''')
        setAutoIncrement('tmp_obs', '(select max(obs_id)+1 from obs)')
    }

    void migrate_tmp_obs() {
        executeMysql("Fill in patient ID from encounter", '''
            UPDATE tmp_obs
            JOIN hivmigration_encounters he ON tmp_obs.source_encounter_id = he.source_encounter_id
            SET tmp_obs.source_patient_id = IFNULL(tmp_obs.source_patient_id, he.source_patient_id);
        ''')
        Long tmpObsCount = (Long) selectMysql("(select count(1) from tmp_obs)", new ScalarHandler())
        Long batchesMigrated = 0
        Long batchSize = 10000
        while (batchesMigrated * batchSize < tmpObsCount) {
            String query = '''
                INSERT INTO obs (
                    obs_id, person_id, encounter_id, obs_group_id, obs_datetime, location_id, concept_id,
                    value_coded, value_drug, value_numeric, value_datetime, value_text, creator, date_created, voided, uuid
                )
                SELECT
                    o.obs_id, p.person_id, e.encounter_id, o.obs_group_id, e.encounter_date, ifnull(e.location_id, 1), q.concept_id,
                    a.concept_id, d.drug_id, o.value_numeric, o.value_datetime, o.value_text, 1, e.date_created, 0, uuid()
                FROM tmp_obs o
                JOIN       hivmigration_encounters e ON o.source_encounter_id = e.source_encounter_id
                JOIN       hivmigration_patients p ON o.source_patient_id = p.source_patient_id
                LEFT JOIN  concept q ON q.uuid = o.concept_uuid
                LEFT JOIN  concept a ON a.uuid = o.value_coded_uuid
                LEFT JOIN  drug d ON d.uuid = o.value_drug_uuid
                ORDER BY o.obs_id
                LIMIT ''' + batchSize + " OFFSET " + (batchesMigrated * batchSize) + ";"
            log.info(query)
            executeMysql("Migrate tmp_obs to obs", query)
            log.info("Obs migrated: " + batchesMigrated * batchSize + " / " + tmpObsCount)
        }
    }
}
