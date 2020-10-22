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

        executeMysql('''
            DROP FUNCTION IF EXISTS is_number;
            CREATE FUNCTION is_number (_value text) RETURNS boolean DETERMINISTIC
            RETURN concat('', _value * 1) = _value;
        ''')

        executeMysql("Create table for logging data irregularities", '''
            CREATE TABLE IF NOT EXISTS hivmigration_data_warnings (
                id INT PRIMARY KEY AUTO_INCREMENT,
                patient_id INT,
                encounter_id INT,
                encounter_date DATE,
                field_name VARCHAR(100),
                field_value VARCHAR(1000),
                note VARCHAR(1000)
            );
        ''')
    }

    @Override
    def void revert() {
        executeMysql("DROP TABLE IF EXISTS hivmigration_data_warnings;")
    }
}
