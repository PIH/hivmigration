package org.pih.hivmigration.etl.sql

/**
 * Adds fake data of yes on one key population for 20% of patients, within
 * that 20%:
 *
 * +--------------+--------+-----+--------+-----+
 * |              | M < 16 |  M  | F < 16 |  F  |
 * +--------------+--------+-----+--------+-----+
 * | MSM          | 5%     | 50% | 0      | 0   |
 * | sex worker   | 0      | 5%  | 0      | 15% |
 * | prisoner     | 1%     | 10% | 0      | 5%  |
 * | transgender  | 0      | 2%  | 0      | 1%  |
 * | IV drug user | 5%     | 20% | 1%     | 25% |
 * +--------------+--------+-----+--------+-----+
 *
 * So the probability of a yes from the total population is
 *
 * +--------------+--------+------+--------+------+
 * |              | M < 16 |  M   | F < 16 |  F   |
 * +--------------+--------+------+--------+------+
 * | MSM          |    .01 |  .10 |      0 |    0 |
 * | sex worker   |      0 |  .01 |      0 |  .03 |
 * | prisoner     |   .002 |  .02 |      0 |  .01 |
 * | transgender  |      0 | .004 |      0 | .002 |
 * | IV drug user |    .01 |  .04 |   .002 |  .05 |
 * +--------------+--------+------+--------+------+
 *
 * Add IV drug user as a second item to 20% Add MSM as a second item to 30%
 *
 * For all non-yeses (ie across all others of these 5 data points across all
 * patients in the system that we have not set to yes via the above rules),
 * randomly have 25% of the answers be unknown and the rest no.
 *
 */
class SampleDataMigrator extends ObsMigrator {

    @Override
    void migrate() {

        executeMysql("Set up all the functions and procedures", '''
            DROP FUNCTION IF EXISTS p_hash;
            DELIMITER //
            CREATE FUNCTION p_hash (_input VARCHAR(120), _iteration INT) RETURNS DECIMAL(3, 3) DETERMINISTIC 
            BEGIN
                RETURN conv(substring(md5(_input), 1, 4 + _iteration), 16, 10) % 1000 / 1000;
            END; //
            DELIMITER ;
            
            DROP FUNCTION IF EXISTS concept_uuid_for_population_name;
            DELIMITER //
            CREATE FUNCTION concept_uuid_for_population_name (_population_name VARCHAR(8)) RETURNS CHAR(38) DETERMINISTIC 
            BEGIN
                RETURN CASE _population_name
                           WHEN 'msm' THEN concept_uuid_from_mapping('CIEL', '160578')
                           WHEN 'sw' THEN concept_uuid_from_mapping('CIEL', '160579')
                           WHEN 'prisoner' THEN concept_uuid_from_mapping('CIEL', '156761')
                           WHEN 'trans' THEN concept_uuid_from_mapping('PIH', '11561')
                           WHEN 'iv' THEN concept_uuid_from_mapping('CIEL', '105')
                    END;
            END; //
            DELIMITER ;
            
            DROP PROCEDURE IF EXISTS add_sample_data;
            DELIMITER $$ ;
            CREATE PROCEDURE add_sample_data(_gender CHAR(1), _under16 BOOL, _population_name VARCHAR(8), _pMin DECIMAL(5, 4), _pMax DECIMAL(5, 4))
            BEGIN
                INSERT INTO tmp_obs
                (source_encounter_id, concept_uuid, value_coded_uuid)
                SELECT he.source_encounter_id,
                       concept_uuid_for_population_name(_population_name),
                       concept_uuid_from_mapping('PIH', 'YES')
                FROM (SELECT person_id,
                             gender,
                             birthdate,
                             source_patient_id,
                             p_hash(CONCAT(first_name, last_name), 1) as p1
                      FROM hivmigration_patients
                     ) hp
                         JOIN hivmigration_encounters he ON hp.source_patient_id = he.source_patient_id AND he.source_encounter_type = 'intake'
                WHERE gender = _gender
                  AND IF(_under16, age_at_enc(person_id, encounter_id) < 16, age_at_enc(person_id, encounter_id) >= 16)
                  AND p1 > _pMin
                  AND p1 <= _pMax;
            END $$
            DELIMITER ;
            
            DROP PROCEDURE IF EXISTS fill_unknowns;
            DELIMITER $$ ;
            CREATE PROCEDURE fill_unknowns(_population_name VARCHAR(8))
            BEGIN
                INSERT INTO tmp_obs
                (source_encounter_id, concept_uuid, value_coded_uuid)
                SELECT he.source_encounter_id,
                       concept_uuid_for_population_name(_population_name),
                       concept_uuid_from_mapping('PIH', 'UNKNOWN')
                FROM hivmigration_patients hp
                         JOIN hivmigration_encounters he ON hp.source_patient_id = he.source_patient_id AND he.source_encounter_type = 'intake\'
                         LEFT JOIN tmp_obs o on he.source_encounter_id = o.source_encounter_id
                GROUP BY source_encounter_id
                HAVING COUNT(CASE WHEN o.concept_uuid = concept_uuid_for_population_name(_population_name) THEN 1 END) = 0
                   AND p_hash(LEFT(GROUP_CONCAT(first_name, last_name), 120), 2) < 0.25;
            END $$
            DELIMITER ;
        ''')

        create_tmp_obs_table()

        executeMysql("Add YES data", '''
            -- M <16
            call add_sample_data('M', TRUE, 'msm', 0, 0.01);
            call add_sample_data('M', TRUE, 'prisoner', 0.01, 0.012);
            call add_sample_data('M', TRUE, 'iv', 0.012, 0.022);
            
            -- adult M
            call add_sample_data('M', FALSE, 'msm', 0, 0.1);
            call add_sample_data('M', FALSE, 'sw', 0.1, 0.11);
            call add_sample_data('M', FALSE, 'prisoner', 0.11, 0.13);
            call add_sample_data('M', FALSE, 'trans', 0.13, 0.134);
            call add_sample_data('M', FALSE, 'iv', 0.134, 0.174);
            
            -- F <16
            call add_sample_data('F', TRUE, 'iv', 0, 0.002);
            
            -- adult F
            call add_sample_data('F', FALSE, 'sw', 0, 0.03);
            call add_sample_data('F', FALSE, 'prisoner', 0.03, 0.04);
            call add_sample_data('F', FALSE, 'trans', 0.04, 0.042);
            call add_sample_data('F', FALSE, 'iv', 0.042, 0.092);
        ''')

        executeMysql("Add second YESes to some", '''
            -- 30% have MSM as second
            INSERT INTO tmp_obs
            (source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT he.source_encounter_id,
                   concept_uuid_for_population_name('msm'),
                   concept_uuid_from_mapping('PIH', 'YES')
            FROM hivmigration_patients hp
            JOIN hivmigration_encounters he ON hp.source_patient_id = he.source_patient_id AND he.source_encounter_type = 'intake\'
            JOIN tmp_obs o on he.source_encounter_id = o.source_encounter_id
            GROUP BY source_encounter_id
            HAVING COUNT(CASE WHEN o.concept_uuid = concept_uuid_for_population_name('msm') THEN 1 END) = 0
               AND (COUNT(CASE WHEN o.concept_uuid = concept_uuid_for_population_name('sw') THEN 1 END) = 1
                    OR COUNT(CASE WHEN o.concept_uuid = concept_uuid_for_population_name('prisoner') THEN 1 END) = 1
                    OR COUNT(CASE WHEN o.concept_uuid = concept_uuid_for_population_name('trans') THEN 1 END) = 1
                    OR COUNT(CASE WHEN o.concept_uuid = concept_uuid_for_population_name('iv') THEN 1 END) = 1)
               AND p_hash(GROUP_CONCAT(first_name, last_name), 3) < 0.3;
            
            -- 20% have IV as second
            INSERT INTO tmp_obs
            (source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT he.source_encounter_id,
                   concept_uuid_for_population_name('iv'),
                   concept_uuid_from_mapping('PIH', 'YES')
            FROM hivmigration_patients hp
                     JOIN hivmigration_encounters he ON hp.source_patient_id = he.source_patient_id AND he.source_encounter_type = 'intake\'
                     JOIN tmp_obs o on he.source_encounter_id = o.source_encounter_id
            GROUP BY source_encounter_id
            HAVING COUNT(CASE WHEN o.concept_uuid = concept_uuid_for_population_name('iv') THEN 1 END) = 0
               AND (COUNT(CASE WHEN o.concept_uuid = concept_uuid_for_population_name('sw') THEN 1 END) = 1
                OR COUNT(CASE WHEN o.concept_uuid = concept_uuid_for_population_name('prisoner') THEN 1 END) = 1
                OR COUNT(CASE WHEN o.concept_uuid = concept_uuid_for_population_name('trans') THEN 1 END) = 1
                OR COUNT(CASE WHEN o.concept_uuid = concept_uuid_for_population_name('msm') THEN 1 END) = 1)
               AND p_hash(GROUP_CONCAT(first_name, last_name), 4) < 0.2;
        ''')

        executeMysql("call fill_unknowns('msm');")
        executeMysql("call fill_unknowns('sw');")
        executeMysql("call fill_unknowns('prisoner');")
        executeMysql("call fill_unknowns('trans');")
        executeMysql("call fill_unknowns('iv');")

        migrate_tmp_obs()

    }

    @Override
    void revert() {
        clearTable("obs")
    }
}
