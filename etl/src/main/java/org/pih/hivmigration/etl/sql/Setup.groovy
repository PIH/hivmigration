package org.pih.hivmigration.etl.sql

class Setup extends SqlMigrator {

    @Override
    def void migrate() {

        executeMysql("Create function concept_uuid_from_mapping", '''
            DROP FUNCTION IF EXISTS concept_uuid_from_mapping;
            DELIMITER //
            CREATE FUNCTION concept_uuid_from_mapping ( _source varchar(80), _code varchar(80))
            RETURNS char(38) DETERMINISTIC
            BEGIN
                RETURN (SELECT uuid FROM concept WHERE concept_id = concept_from_mapping(_source, _code));
            END;
            DELIMITER ;
        ''')

        executeMysql("Create function is_number", '''
            -- Checks whether a string value represents a number. Allows surrounding whitespace.
            -- Allows an arbitrary number of decimal points or commas.
            -- Recommended to use with 'extract_number', e.g.
            --   SELECT extract_number(result) FROM tbl WHERE is_number(result)
            DROP FUNCTION IF EXISTS is_number;
            CREATE FUNCTION is_number (_value text) RETURNS boolean DETERMINISTIC
                RETURN _value REGEXP '^[0-9\\.,]+$' AND _value REGEXP '[0-9]';
        ''')

        executeMysql("Create function extract_number", '''
            -- extracts the first number in any string. Normalizes commas to decimal points.
            -- e.g.
            --   'hey 9.0 33 f'  -->  '9.0'
            --   '04,4%' --> '4.4'
            -- returns NULL for NULL input
            -- inspired by https://stackoverflow.com/a/991802/1464495
            DROP FUNCTION IF EXISTS extract_number;
            DELIMITER |
            CREATE FUNCTION extract_number( str CHAR(32) ) RETURNS CHAR(32) DETERMINISTIC
            BEGIN
                DECLARE i, len SMALLINT DEFAULT 1;
                DECLARE ret CHAR(32) DEFAULT '';
                DECLARE c CHAR(1);
            
                IF str IS NULL
                THEN
                    RETURN NULL;
                END IF;
            
                SET len = CHAR_LENGTH( str );
                REPEAT
                    BEGIN
                        SET c = MID( str, i, 1 );
                        IF c REGEXP '[0-9\\.,]' THEN
                            SET ret=CONCAT(ret, c);
                        END IF;
                        SET i = i + 1;
                    END;
                UNTIL i > len OR (CHAR_LENGTH(ret) > 0 AND c NOT REGEXP '[0-9\\.,]') END REPEAT;
                SET ret = TRIM(TRAILING '.' FROM TRIM(LEADING '0' FROM REPLACE(ret, ',', '.')));
                RETURN IF(ret = '', '0', ret);
            END |
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
                warning_details VARCHAR(1000),
                flag_for_review BOOLEAN DEFAULT FALSE
            );
        ''')
    }

    @Override
    def void revert() {
        executeMysql("DROP TABLE IF EXISTS hivmigration_data_warnings;")
    }
}
