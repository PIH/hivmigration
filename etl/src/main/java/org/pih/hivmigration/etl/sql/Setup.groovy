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
            -- from https://stackoverflow.com/a/5065007/1464495
            DROP FUNCTION IF EXISTS is_number;
            CREATE FUNCTION is_number (_value text) RETURNS boolean DETERMINISTIC
            RETURN concat('', _value * 1) = _value;
        ''')

        executeMysql('''
            -- from https://stackoverflow.com/a/991802/1464495
            DROP FUNCTION IF EXISTS extract_number;
            DELIMITER |
            CREATE FUNCTION extract_number( str CHAR(32) ) RETURNS CHAR(32)
            BEGIN
                DECLARE i, len SMALLINT DEFAULT 1;
                DECLARE ret CHAR(32) DEFAULT '';
                DECLARE c CHAR(1);
            
                IF str IS NULL
                THEN
                    RETURN "";
                END IF;
            
                SET len = CHAR_LENGTH( str );
                REPEAT
                    BEGIN
                        SET c = MID( str, i, 1 );
                        IF c BETWEEN '0' AND '9' THEN
                            SET ret=CONCAT(ret,c);
                        END IF;
                        SET i = i + 1;
                    END;
                UNTIL i > len END REPEAT;
                RETURN ret;
            END |
            DELIMITER ;
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
