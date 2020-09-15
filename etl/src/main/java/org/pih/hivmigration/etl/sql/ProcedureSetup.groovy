package org.pih.hivmigration.etl.sql

class ProcedureSetup extends SqlMigrator {

    @Override
    def void migrate() {
        executeMysql("Create procedures for migrating obs", '''
            DROP PROCEDURE IF EXISTS create_tmp_obs_table;
            DROP PROCEDURE IF EXISTS migrate_tmp_obs;
            DELIMITER $$ ;
            CREATE PROCEDURE create_tmp_obs_table()
            BEGIN
                DROP TABLE IF EXISTS tmp_obs;
                CREATE TABLE tmp_obs (
                    obs_id INT PRIMARY KEY AUTO_INCREMENT,
                    obs_group_id INT,
                    source_patient_id INT,
                    source_encounter_id INT,
                    concept_uuid CHAR(38),
                    value_coded_uuid CHAR(38),
                    drug_uuid CHAR(38),
                    value_datetime DATETIME,
                    value_numeric DOUBLE,
                    value_text TEXT
                );
            END $$
            CREATE PROCEDURE migrate_tmp_obs()
            BEGIN
                INSERT INTO obs (
                    obs_id, person_id, encounter_id, obs_group_id, obs_datetime, location_id, concept_id,
                    value_coded, value_drug, value_numeric, value_datetime, value_text, creator, date_created, voided, uuid
                )
                SELECT
                    o.obs_id, p.person_id, e.encounter_id, o.obs_group_id, e.encounter_date, e.location_id, q.concept_id,
                    a.concept_id, d.drug_id, o.value_numeric, o.value_datetime, o.value_text, 1, e.date_created, 0, uuid()
                FROM tmp_obs o
                    JOIN       hivmigration_patients p ON o.source_patient_id = p.source_patient_id
                    JOIN       hivmigration_encounters e ON o.source_encounter_id = e.source_encounter_id
                    LEFT JOIN  concept q ON q.uuid = o.concept_uuid
                    LEFT JOIN  concept a ON a.uuid = o.value_coded_uuid 
                    LEFT JOIN  drug d ON d.uuid = o.drug_uuid;                    
            END $$
            DELIMITER ;
        ''')
    }

    @Override
    def void revert() {
    }
}
