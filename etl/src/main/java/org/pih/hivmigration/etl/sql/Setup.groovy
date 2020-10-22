package org.pih.hivmigration.etl.sql

class Setup extends SqlMigrator {

    @Override
    def void migrate() {

        executeMysql('''
            DROP FUNCTION IF EXISTS concept_uuid_from_mapping;
            DELIMITER //
            CREATE FUNCTION concept_uuid_from_mapping ( _source varchar(80), _code varchar(80))
            RETURNS char(38) DETERMINISTIC
            BEGIN
                RETURN (SELECT uuid FROM concept WHERE concept_id = concept_from_mapping(_source, _code));
            END;
            DELIMITER ;
        ''')

        executeMysql("Create table for logging data irregularities", '''
            CREATE TABLE IF NOT EXISTS hivmigration_data_warnings (
                id INT PRIMARY KEY AUTO_INCREMENT,
                openmrs_patient_id INT,
                openmrs_encounter_id INT,
                hiv_emr_encounter_id INT,
                zl_emr_id VARCHAR(255),
                hivemr_v1_id INT,
                hiv_dossier_id VARCHAR(255),
                encounter_date DATE,
                field_name VARCHAR(100),
                field_value VARCHAR(1000),
                warning_type VARCHAR(255),
                warning_details VARCHAR(1000)
            );
        ''')
    }

    @Override
    def void revert() {
        executeMysql("DROP TABLE IF EXISTS hivmigration_data_warnings;")
    }
}
