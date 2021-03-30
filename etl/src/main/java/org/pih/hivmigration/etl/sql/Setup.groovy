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

        executeMysql("Create function concept_name_from_uuid", '''
            DROP FUNCTION IF EXISTS concept_name_from_uuid;
            DELIMITER //
            CREATE FUNCTION concept_name_from_uuid ( _uuid varchar(38))
                RETURNS varchar(128) DETERMINISTIC
            BEGIN
                RETURN (SELECT name FROM concept_name cn JOIN concept c on cn.concept_id = c.concept_id
                        WHERE c.uuid = _uuid AND cn.locale = 'en' AND cn.locale_preferred = 1 AND cn.voided = 0);
            END;
            DELIMITER ;
        ''')

        executeMysql("Create function uuid_hash", '''
            -- A deterministic way to produce something UUID-ish (doesn't meet the full spec, but good enough for OpenMRS)
            -- Make sure to pass it adequately unique input!
            drop function if exists uuid_hash;
            delimiter //
            create function uuid_hash (_value text) returns char(36) deterministic
            begin
                SET @hash = (SELECT LOWER(MD5(_value)));
                return LOWER(CONCAT(
                        SUBSTR(@hash, 1, 8), '-',
                        SUBSTR(@hash, 9, 4), '-',
                        SUBSTR(@hash, 13, 4), '-',
                        SUBSTR(@hash, 17, 4), '-',
                        SUBSTR(@hash, 21)
                    ));
            end;
            delimiter ;
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

        executeMysql("Create function try_to_fix_date", '''
            DROP FUNCTION IF EXISTS try_to_fix_date;
            DELIMITER |
            CREATE FUNCTION try_to_fix_date( date_string CHAR(12) ) RETURNS DATE DETERMINISTIC
            BEGIN
                RETURN IF(  -- fix day value if too large for month
                       DAY(STR_TO_DATE(date_string, '%Y-%m-%d')) > DAY(LAST_DAY(STR_TO_DATE(date_string,'%Y-%m-%d'))),
                       CONCAT(LEFT(date_string, 5), CAST(SUBSTR(date_string, 6, 2) AS UNSIGNED)+1, '-01'),
                       IF(  -- fix month value > 12
                                   MONTH(STR_TO_DATE(date_string, '%Y-%m-%d')) <= 12,
                                   date_string,
                                   CONCAT(LEFT(date_string, 5), '12-01')));
            END |
            DELIMITER ;
        ''')

        executeMysql("Create table for logging data irregularities", '''
            CREATE TABLE IF NOT EXISTS hivmigration_data_warnings (
                id INT PRIMARY KEY AUTO_INCREMENT,
                openmrs_patient_id INT,
                openmrs_encounter_id INT,
                hiv_emr_encounter_id INT,  -- interpolated
                zl_emr_id VARCHAR(255),  -- interpolated
                hivemr_v1_id INT,  -- interpolated
                hiv_dossier_id VARCHAR(255),  -- interpolated
                hivemr_v1_infant_id int,  -- interpolated
                hivemr_v1_infant_code varchar(20),  -- interpolated
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
